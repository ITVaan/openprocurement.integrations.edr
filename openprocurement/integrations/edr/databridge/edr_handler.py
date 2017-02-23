# -*- coding: utf-8 -*-
from gevent import monkey
monkey.patch_all()

try:
    import urllib3.contrib.pyopenssl
    urllib3.contrib.pyopenssl.inject_into_urllib3()
except ImportError:
    pass

import logging.config
import gevent

from openprocurement.integrations.edr.journal_msg_ids import (
    DATABRIDGE_GET_TENDER_FROM_QUEUE,
    DATABRIDGE_EMPTY_RESPONSE, DATABRIDGE_START,
    DATABRIDGE_UNAUTHORIZED_EDR, DATABRIDGE_SUCCESS_CREATE_FILE)
from openprocurement.integrations.edr.databridge.utils import Data, generate_req_id, journal_context, validate_param

logger = logging.getLogger("openprocurement.integrations.edr.databridge")


class EdrHandler(object):
    """ Edr API Data Bridge """

    required_fields = ['names', 'founders', 'management', 'activity_kinds', 'address', 'bankruptcy']

    def __init__(self, edrApiClient, data_queue, subjects_queue, upload_file_queue, delay=15):
        super(EdrHandler, self).__init__()

        # init clients
        self.edrApiClient = edrApiClient

        # init queues for workers
        self.data_queue = data_queue
        self.subjects_queue = subjects_queue
        self.upload_file_queue = upload_file_queue

        # blockers
        self.until_too_many_requests_event = gevent.event.Event()

        self.until_too_many_requests_event.set()

        self.delay = delay

    def get_subject_id(self):
        while True:
            try:
                tender_data = self.data_queue.get()
                logger.info('Get tender {} from data_queue'.format(tender_data.tender_id),
                            extra=journal_context({"MESSAGE_ID": DATABRIDGE_GET_TENDER_FROM_QUEUE},
                                                  params={"TENDER_ID": tender_data.tender_id}))
            except Exception as e:
                logger.warning('Fail to get tender {} with {} id {} from edrpou queue'.format(
                    tender_data.tender_id, tender_data.item_name, tender_data.item_id),
                            extra=journal_context(params={"TENDER_ID": tender_data.tender_id}))
                logger.exception(e)
                logger.info('Put tender {} with {} id {} back to tenders queue'.format(
                                tender_data.tender_id, tender_data.item_name, tender_data.item_id),
                            extra=journal_context(params={"TENDER_ID": tender_data.tender_id}))
                self.data_queue.put((tender_data.tender_id, tender_data.item_id, tender_data.code))
                gevent.sleep(self.delay)
            else:
                gevent.wait([self.until_too_many_requests_event])
                response = self.edrApiClient.get_subject(validate_param(tender_data.code), tender_data.code)
                if response.status_code == 200:
                    # Create new Data object. Write to Data.code list of subject ids from EDR.
                    # List because EDR can return 0, 1 or 2 values to our reques
                    data = Data(tender_data.tender_id, tender_data.item_id,
                                [subject['id'] for subject in response.json()], tender_data.item_name, None)
                    self.subjects_queue.put(data)
                    logger.info('Put tender {} {} {} to subjects_queue.'.format(tender_data.tender_id,
                                                                                tender_data.item_name,
                                                                                tender_data.item_id))
                else:
                    self.handle_status_response(response, tender_data.tender_id)

    def get_subject_details(self):
        while True:
            try:
                tender_data = self.subjects_queue.get()
                logger.info('Get subject {}  tender {} from data_queue'.format(tender_data.code, tender_data.tender_id),
                            extra=journal_context({"MESSAGE_ID": DATABRIDGE_GET_TENDER_FROM_QUEUE},
                                                  params={"TENDER_ID": tender_data.tender_id}))
            except Exception as e:
                logger.warning('Fail to get tender {} with {} id {} from edrpou queue'.format(
                                tender_data.tender_id, tender_data.item_name, tender_data.item_id),
                            extra=journal_context(params={"TENDER_ID": tender_data.tender_id}))
                logger.exception(e)
                logger.info('Put tender {} with {} id {} back to tenders queue'.format(
                                tender_data.tender_id, tender_data.item_name, tender_data.item_id),
                            extra=journal_context(params={"TENDER_ID": tender_data.tender_id}))
                self.subjects_queue.put((tender_data.tender_id, tender_data.item_id, tender_data.code))
                gevent.sleep(self.delay)
            else:
                gevent.wait([self.until_too_many_requests_event])
                if not tender_data.code:
                    details = {'error': 'Couldn\'t find this code in EDR.'}
                    logger.info('Empty response for tender {}.'.format(tender_data.tender_id),
                                extra=journal_context({"MESSAGE_ID": DATABRIDGE_EMPTY_RESPONSE},
                                                      params={"TENDER_ID": tender_data.tender_id}))
                for subject_id in tender_data.code:
                    response = self.edrApiClient.get_subject_details(subject_id)
                    if response.status_code == 200:
                        details = {key: value for key, value in response.json().items() if key in self.required_fields}
                    else:
                        self.handle_status_response(response, tender_data.tender_id)
                data = Data(tender_data.tender_id, tender_data.item_id,
                            tender_data.code, tender_data.item_name, details)
                self.upload_file_queue.put(data)
                logger.info('Successfully created file for tender {} {} {}'.format(
                            tender_data.tender_id, tender_data.item_name, tender_data.item_id),
                            extra=journal_context({"MESSAGE_ID": DATABRIDGE_SUCCESS_CREATE_FILE},
                                                  params={"TENDER_ID": tender_data.tender_id}))

    def handle_status_response(self, response, tender_id):
        if response.status_code == 401:
            logger.info('Not Authorized (invalid token) for tender {}'.format(tender_id),
                        extra=journal_context({"MESSAGE_ID": DATABRIDGE_UNAUTHORIZED_EDR}, {"TENDER_ID": tender_id}))
            raise Exception('Invalid EDR API token')

        elif response.status_code == 429:
            self.until_too_many_requests_event.clear()
            gevent.sleep(response.headers.get('Retry-After', self.delay))
            self.until_too_many_requests_event.set()

        elif response.status_code == 402:
            logger.info('Payment required for requesting info to EDR. '
                        'Error description: {err}'.format(err=response.text),
                        extra=journal_context(params={"TENDER_ID": tender_id}))
        else:
            logger.info('Error appeared while requesting to EDR. '
                        'Description: {err}'.format(err=response.text),
                        extra=journal_context(params={"TENDER_ID": tender_id}))

    def run(self):
        logger.info('Start Filter Tenders Bridge',
                    extra=journal_context({"MESSAGE_ID": DATABRIDGE_START},
                                          {}))
        get_subject_id = gevent.spawn(self.get_subject_id)
        get_subject_details = gevent.spawn(self.get_subject_details)
        try:
            while True:
                gevent.sleep(self.delay)
                if get_subject_id.dead:
                    logger.warning("EDR handler worker get_subject_id dead try restart")
                    get_subject_id = gevent.spawn(self.get_subject_id)
                    logger.info("EDR handler worker get_subject_id is up")
                if get_subject_details.dead:
                    logger.warning("EDR handler worker get_subject_id dead try restart")
                    get_subject_details = gevent.spawn(self.get_subject_details)
                    logger.info("EDR handler worker get_subject_id is up")
        except Exception as e:
            logger.error(e)
            get_subject_details.kill(timeout=5)
            get_subject_id.kill(timeout=5)
