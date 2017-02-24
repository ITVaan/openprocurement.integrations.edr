# -*- coding: utf-8 -*-
from gevent import monkey
from gevent.queue import Queue
from retrying import retry
monkey.patch_all()

try:
    import urllib3.contrib.pyopenssl
    urllib3.contrib.pyopenssl.inject_into_urllib3()
except ImportError:
    pass

import logging.config
import gevent

from openprocurement.integrations.edr.databridge.journal_msg_ids import (
    DATABRIDGE_GET_TENDER_FROM_QUEUE, DATABRIDGE_START_EDR_HANDLER, DATABRIDGE_RESTART_EDR_HANDLER_GET_ID,
    DATABRIDGE_UNAUTHORIZED_EDR, DATABRIDGE_SUCCESS_CREATE_FILE, DATABRIDGE_RESTART_EDR_HANDLER_GET_DETAILS,
    DATABRIDGE_EMPTY_RESPONSE)
from openprocurement.integrations.edr.databridge.utils import Data, journal_context, validate_param

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

        # retry queues for workers
        self.retry_data_queue = Queue(maxsize=500)
        self.retry_subjects_queue = Queue(maxsize=500)

        # blockers
        self.until_too_many_requests_event = gevent.event.Event()

        self.until_too_many_requests_event.set()

        self.delay = delay

    def get_subject_id(self):
        while True:
            tender_data = self.data_queue.get()
            logger.info('Get tender {} from data_queue'.format(tender_data.tender_id),
                        extra=journal_context({"MESSAGE_ID": DATABRIDGE_GET_TENDER_FROM_QUEUE},
                                              params={"TENDER_ID": tender_data.tender_id}))
            gevent.wait([self.until_too_many_requests_event])
            response = self.edrApiClient.get_subject(validate_param(tender_data.code), tender_data.code)
            if response.status_code == 200:
                if not response.json():
                    details = {'error': 'Couldn\'t find this code in EDR.'}
                    logger.info('Empty response for tender {}.'.format(tender_data.tender_id),
                                extra=journal_context({"MESSAGE_ID": DATABRIDGE_EMPTY_RESPONSE},
                                                      params={"TENDER_ID": tender_data.tender_id}))
                    data = Data(tender_data.tender_id, tender_data.item_id, tender_data.code,
                                tender_data.item_name, response.json(), details)
                    self.upload_file_queue.put(data)
                # Create new Data object. Write to Data.code list of subject ids from EDR.
                # List because EDR can return 0, 1 or 2 values to our reques
                data = Data(tender_data.tender_id, tender_data.item_id, tender_data.code,
                            tender_data.item_name, [subject['id'] for subject in response.json()], None)
                self.subjects_queue.put(data)
                logger.info('Put tender {} {} {} to subjects_queue.'.format(tender_data.tender_id,
                                                                            tender_data.item_name,
                                                                            tender_data.item_id))
            else:
                self.handle_status_response(response, tender_data.tender_id)
                self.retry_data_queue.put(tender_data)
                logger.info('Put tender {} with {} id {} to retry data queue'.format(
                    tender_data.tender_id, tender_data.item_name, tender_data.item_id),
                    extra=journal_context(params={"TENDER_ID": tender_data.tender_id}))
                gevent.sleep(0)

    def retry_get_subject_id(self):
        tender_data = self.retry_data_queue.get()
        logger.info('Get tender {} from retry_data_queue'.format(tender_data.tender_id),
                    extra=journal_context({"MESSAGE_ID": DATABRIDGE_GET_TENDER_FROM_QUEUE},
                                          params={"TENDER_ID": tender_data.tender_id}))
        gevent.wait([self.until_too_many_requests_event])
        try:
            response = self.get_subject_request(validate_param(tender_data.code), tender_data.code)
        except Exception:
            self.handle_status_response(response, tender_data.tender_id)
            self.retry_data_queue.put(tender_data)
            logger.info('Put tender {} with {} id {} to retry data queue'.format(
                tender_data.tender_id, tender_data.item_name, tender_data.item_id),
                extra=journal_context(params={"TENDER_ID": tender_data.tender_id}))
            gevent.sleep(0)
        else:
            if not response.json():
                details = {'error': 'Couldn\'t find this code in EDR.'}
                logger.info('Empty response for tender {}.'.format(tender_data.tender_id),
                            extra=journal_context({"MESSAGE_ID": DATABRIDGE_EMPTY_RESPONSE},
                                                  params={"TENDER_ID": tender_data.tender_id}))
                data = Data(tender_data.tender_id, tender_data.item_id, tender_data.code,
                            tender_data.item_name, response.json(), details)
                self.upload_file_queue.put(data)
            # Create new Data object. Write to Data.code list of subject ids from EDR.
            # List because EDR can return 0, 1 or 2 values to our reques
            data = Data(tender_data.tender_id, tender_data.item_id, tender_data.code,
                        tender_data.item_name, [subject['id'] for subject in response.json()], None)
            self.subjects_queue.put(data)
            logger.info('Put tender {} {} {} to subjects_queue.'.format(tender_data.tender_id,
                                                                        tender_data.item_name,
                                                                        tender_data.item_id))

    @retry(stop_max_attempt_number=5, wait_exponential_multiplier=1000)
    def get_subject_request(self, param, code):
        response = self.edrApiClient.get_subject(param, code)
        if response.status_code != 200:
            raise Exception('Unsuccessful retry request to EDR.')
        return response

    def get_subject_details(self):
        while True:
            tender_data = self.subjects_queue.get()
            logger.info('Get subjects ids {}  tender {} from subjects queue'.format(tender_data.subject_ids, tender_data.tender_id),
                        extra=journal_context({"MESSAGE_ID": DATABRIDGE_GET_TENDER_FROM_QUEUE},
                                              params={"TENDER_ID": tender_data.tender_id}))
            gevent.wait([self.until_too_many_requests_event])
            for subject_id in tender_data.subject_ids:
                response = self.edrApiClient.get_subject_details(subject_id)
                if response.status_code == 200:
                    data = Data(tender_data.tender_id, tender_data.item_id, tender_data.code,
                                tender_data.item_name, tender_data.subject_ids,
                                {key: value for key, value in response.json().items() if key in self.required_fields})
                    self.upload_file_queue.put(data)
                    logger.info('Successfully created file for tender {} {} {}'.format(
                        tender_data.tender_id, tender_data.item_name, tender_data.item_id),
                        extra=journal_context({"MESSAGE_ID": DATABRIDGE_SUCCESS_CREATE_FILE},
                                              params={"TENDER_ID": tender_data.tender_id}))
                    tender_data.subject_ids.remove(subject_id)  # remove from list subject_id that have successful response
                else:
                    self.retry_subjects_queue.put(tender_data)
                    logger.info('Put tender {} with {} id {} to retry_subjects_queue'.format(
                                tender_data.tender_id, tender_data.item_name, tender_data.item_id),
                            extra=journal_context(params={"TENDER_ID": tender_data.tender_id}))
                    gevent.sleep(0)

    def retry_get_subject_details(self):
        while True:
            tender_data = self.retry_subjects_queue.get()
            logger.info('Get subjects ids {}  tender {} from retry subjects queue'.format(tender_data.subject_ids,
                                                                                    tender_data.tender_id),
                        extra=journal_context({"MESSAGE_ID": DATABRIDGE_GET_TENDER_FROM_QUEUE},
                                              params={"TENDER_ID": tender_data.tender_id}))
            gevent.wait([self.until_too_many_requests_event])
            for subject_id in tender_data.subject_ids:
                try:
                    response = self.edrApiClient.get_subject_details(subject_id)
                except Exception:
                    self.retry_subjects_queue.put(tender_data)
                    logger.info('Put tender {} with {} id {} to retry_subjects_queue'.format(
                        tender_data.tender_id, tender_data.item_name, tender_data.item_id),
                        extra=journal_context(params={"TENDER_ID": tender_data.tender_id}))
                    gevent.sleep(0)
                else:
                    data = Data(tender_data.tender_id, tender_data.item_id, tender_data.code,
                                tender_data.item_name, tender_data.subject_ids,
                                {key: value for key, value in response.json().items() if key in self.required_fields})
                    self.upload_file_queue.put(data)
                    logger.info('Successfully created file for tender {} {} {}'.format(
                        tender_data.tender_id, tender_data.item_name, tender_data.item_id),
                        extra=journal_context({"MESSAGE_ID": DATABRIDGE_SUCCESS_CREATE_FILE},
                                              params={"TENDER_ID": tender_data.tender_id}))
                    tender_data.subject_ids.remove(subject_id)  # remove from list subject_id that have successful response

    @retry(stop_max_attempt_number=5, wait_exponential_multiplier=1000)
    def get_subject_details_request(self, subject_id):
        response = self.edrApiClient.get_subject_details(subject_id)
        if response.status_code != 200:
            raise Exception('Unsuccessful retry request to EDR.')
        return response

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
            raise Exception('Payment required for requesting info to EDR.')
        else:
            logger.info('Error appeared while requesting to EDR. '
                        'Description: {err}'.format(err=response.text),
                        extra=journal_context(params={"TENDER_ID": tender_id}))

    def run(self):
        logger.info('Start EDR Handler', extra=journal_context({"MESSAGE_ID": DATABRIDGE_START_EDR_HANDLER}, {}))
        get_subject_id = gevent.spawn(self.get_subject_id)
        get_subject_details = gevent.spawn(self.get_subject_details)
        try:
            while True:
                gevent.sleep(self.delay)
                if get_subject_id.dead:
                    logger.warning("EDR handler worker get_subject_id dead try restart",
                                   extra=journal_context({"MESSAGE_ID": DATABRIDGE_RESTART_EDR_HANDLER_GET_ID}, {}))
                    get_subject_id = gevent.spawn(self.get_subject_id)
                    logger.info("EDR handler worker get_subject_id is up")
                if get_subject_details.dead:
                    logger.warning("EDR handler worker get_subject_details dead try restart",
                                   extra=journal_context({"MESSAGE_ID": DATABRIDGE_RESTART_EDR_HANDLER_GET_DETAILS}, {}))
                    get_subject_details = gevent.spawn(self.get_subject_details)
                    logger.info("EDR handler worker get_subject_id is up")
        except Exception as e:
            logger.error(e)
            get_subject_details.kill(timeout=5)
            get_subject_id.kill(timeout=5)
