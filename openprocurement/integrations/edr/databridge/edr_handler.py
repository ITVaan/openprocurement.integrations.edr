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
from datetime import datetime
from gevent import Greenlet, spawn

from openprocurement.integrations.edr.databridge.journal_msg_ids import (
    DATABRIDGE_GET_TENDER_FROM_QUEUE, DATABRIDGE_START_EDR_HANDLER, DATABRIDGE_RESTART_EDR_HANDLER_GET_ID,
    DATABRIDGE_UNAUTHORIZED_EDR, DATABRIDGE_SUCCESS_CREATE_FILE, DATABRIDGE_RESTART_EDR_HANDLER_GET_DETAILS,
    DATABRIDGE_EMPTY_RESPONSE)
from openprocurement.integrations.edr.databridge.utils import Data, journal_context, validate_param

logger = logging.getLogger(__name__)


class EdrHandler(Greenlet):
    """ Edr API Data Bridge """

    required_fields = ['names', 'founders', 'management', 'activity_kinds', 'address', 'bankruptcy']
    error_details = {'error': 'Couldn\'t find this code in EDR.'}

    def __init__(self, edrApiClient, edrpou_codes_queue, edr_ids_queue, upload_file_queue, delay=15):
        super(EdrHandler, self).__init__()
        self.exit = False
        self.start_time = datetime.now()

        # init clients
        self.edrApiClient = edrApiClient

        # init queues for workers
        self.edrpou_codes_queue = edrpou_codes_queue
        self.edr_ids_queue = edr_ids_queue
        self.upload_file_queue = upload_file_queue

        # retry queues for workers
        self.retry_edrpou_codes_queue = Queue(maxsize=500)
        self.retry_edr_ids_queue = Queue(maxsize=500)

        # blockers
        self.until_too_many_requests_event = gevent.event.Event()

        self.until_too_many_requests_event.set()

        self.delay = delay

    def get_edr_id(self):
        """Get data from edrpou_codes_queue; make request to EDR Api, passing EDRPOU (IPN, passport); Received ids is
        put into Data.edr_ids variable; Data variable placed to edr_ids_queue."""
        while True:
            tender_data = self.edrpou_codes_queue.get()
            logger.info('Get tender {} from edrpou_codes_queue'.format(tender_data.tender_id),
                        extra=journal_context({"MESSAGE_ID": DATABRIDGE_GET_TENDER_FROM_QUEUE},
                                              params={"TENDER_ID": tender_data.tender_id}))
            gevent.wait([self.until_too_many_requests_event])
            response = self.edrApiClient.get_subject(validate_param(tender_data.code), tender_data.code)
            if response.status_code == 200:
                if not response.json():
                    logger.info('Empty response for tender {}.'.format(tender_data.tender_id),
                                extra=journal_context({"MESSAGE_ID": DATABRIDGE_EMPTY_RESPONSE},
                                                      params={"TENDER_ID": tender_data.tender_id}))
                    data = Data(tender_data.tender_id, tender_data.item_id, tender_data.code,
                                tender_data.item_name, response.json(), self.error_details)
                    self.upload_file_queue.put(data)  # Given EDRPOU code not found, file with error put into upload_file_queue
                    continue
                # Create new Data object. Write to Data.code list of edr ids from EDR.
                # List because EDR can return 0, 1 or 2 values to our reques
                data = Data(tender_data.tender_id, tender_data.item_id, tender_data.code,
                            tender_data.item_name, [edr_ids['id'] for edr_ids in response.json()], None)
                self.edr_ids_queue.put(data)
                logger.info('Put tender {} {} {} to edr_ids_queue.'.format(tender_data.tender_id,
                                                                           tender_data.item_name,
                                                                           tender_data.item_id))
            else:
                self.handle_status_response(response, tender_data.tender_id)
                self.retry_edrpou_codes_queue.put(tender_data)
                logger.info('Put tender {} with {} id {} to retry_edrpou_codes_queue'.format(
                    tender_data.tender_id, tender_data.item_name, tender_data.item_id),
                    extra=journal_context(params={"TENDER_ID": tender_data.tender_id}))
                gevent.sleep(0)

    def retry_get_edr_id(self):
        """Get data from retry_edrpou_codes_queue; Put data into edr_ids_queue if request is successful, otherwise put
        data back to retry_edrpou_codes_queue."""
        tender_data = self.retry_edrpou_codes_queue.get()
        logger.info('Get tender {} from retry_edrpou_codes_queue'.format(tender_data.tender_id),
                    extra=journal_context({"MESSAGE_ID": DATABRIDGE_GET_TENDER_FROM_QUEUE},
                                          params={"TENDER_ID": tender_data.tender_id}))
        gevent.wait([self.until_too_many_requests_event])
        try:
            response = self.get_edr_id_request(validate_param(tender_data.code), tender_data.code)
        except Exception:
            self.handle_status_response(response, tender_data.tender_id)
            self.retry_edrpou_codes_queue.put(tender_data)
            logger.info('Put tender {} with {} id {} to retry_edrpou_codes_queue'.format(
                tender_data.tender_id, tender_data.item_name, tender_data.item_id),
                extra=journal_context(params={"TENDER_ID": tender_data.tender_id}))
            gevent.sleep(0)
        else:
            if not response.json():
                logger.info('Empty response for tender {}.'.format(tender_data.tender_id),
                            extra=journal_context({"MESSAGE_ID": DATABRIDGE_EMPTY_RESPONSE},
                                                  params={"TENDER_ID": tender_data.tender_id}))
                data = Data(tender_data.tender_id, tender_data.item_id, tender_data.code,
                            tender_data.item_name, response.json(), self.error_details)
                self.upload_file_queue.put(data)
            # Create new Data object. Write to Data.code list of edr ids from EDR.
            # List because EDR can return 0, 1 or 2 values to our request
            data = Data(tender_data.tender_id, tender_data.item_id, tender_data.code,
                        tender_data.item_name, [obj['id'] for obj in response.json()], None)
            self.edr_ids_queue.put(data)
            logger.info('Put tender {} {} {} to edr_ids_queue.'.format(tender_data.tender_id,
                                                                       tender_data.item_name,
                                                                       tender_data.item_id))

    @retry(stop_max_attempt_number=5, wait_exponential_multiplier=1000)
    def get_edr_id_request(self, param, code):
        """Execute request to EDR Api for retry queue objects."""
        response = self.edrApiClient.get_subject(param, code)
        if response.status_code != 200:
            raise Exception('Unsuccessful retry request to EDR.')
        return response

    def get_edr_details(self):
        """Get data from edr_ids_queue; make request to EDR Api for detailed info; Required fields is put to
        Data.file_content variable, Data object is put to upload_file_queue."""
        while True:
            tender_data = self.edr_ids_queue.get()
            logger.info('Get edr ids {}  tender {} from edr_ids_queue'.format(tender_data.edr_ids, tender_data.tender_id),
                        extra=journal_context({"MESSAGE_ID": DATABRIDGE_GET_TENDER_FROM_QUEUE},
                                              params={"TENDER_ID": tender_data.tender_id}))
            gevent.wait([self.until_too_many_requests_event])
            for edr_id in tender_data.edr_ids:
                response = self.edrApiClient.get_subject_details(edr_id)
                if response.status_code == 200:
                    data = Data(tender_data.tender_id, tender_data.item_id, tender_data.code,
                                tender_data.item_name, tender_data.edr_ids,
                                {key: value for key, value in response.json().items() if key in self.required_fields})
                    self.upload_file_queue.put(data)
                    logger.info('Successfully created file for tender {} {} {}'.format(
                        tender_data.tender_id, tender_data.item_name, tender_data.item_id),
                        extra=journal_context({"MESSAGE_ID": DATABRIDGE_SUCCESS_CREATE_FILE},
                                              params={"TENDER_ID": tender_data.tender_id}))
                    tender_data.edr_ids.remove(edr_id)  # remove from list edr_id that have successful response
                else:
                    self.retry_edr_ids_queue.put(tender_data)
                    logger.info('Put tender {} with {} id {} to retry_edr_ids_queue'.format(
                                tender_data.tender_id, tender_data.item_name, tender_data.item_id),
                            extra=journal_context(params={"TENDER_ID": tender_data.tender_id}))
                    gevent.sleep(0)

    def retry_get_edr_details(self):
        """Get data from retry_edr_ids_queue; Put data into upload_file_queue if request is successful, otherwise put
        data back to retry_edr_ids_queue."""
        while True:
            tender_data = self.retry_edr_ids_queue.get()
            logger.info('Get edr ids {}  tender {} from retry_edr_ids_queue'.format(tender_data.edr_ids,
                                                                                    tender_data.tender_id),
                        extra=journal_context({"MESSAGE_ID": DATABRIDGE_GET_TENDER_FROM_QUEUE},
                                              params={"TENDER_ID": tender_data.tender_id}))
            gevent.wait([self.until_too_many_requests_event])
            for edr_id in tender_data.edr_ids:
                try:
                    response = self.get_edr_details_request(edr_id)
                except Exception:
                    self.retry_edr_ids_queue.put(tender_data)
                    logger.info('Put tender {} with {} id {} to retry_edr_ids_queue'.format(
                        tender_data.tender_id, tender_data.item_name, tender_data.item_id),
                        extra=journal_context(params={"TENDER_ID": tender_data.tender_id}))
                    gevent.sleep(0)
                else:
                    data = Data(tender_data.tender_id, tender_data.item_id, tender_data.code,
                                tender_data.item_name, tender_data.edr_ids,
                                {key: value for key, value in response.json().items() if key in self.required_fields})
                    self.upload_file_queue.put(data)
                    logger.info('Successfully created file for tender {} {} {}'.format(
                        tender_data.tender_id, tender_data.item_name, tender_data.item_id),
                        extra=journal_context({"MESSAGE_ID": DATABRIDGE_SUCCESS_CREATE_FILE},
                                              params={"TENDER_ID": tender_data.tender_id}))
                    tender_data.edr_ids.remove(edr_id)  # remove from list edr_id that have successful response

    @retry(stop_max_attempt_number=5, wait_exponential_multiplier=1000)
    def get_edr_details_request(self, edr_id):
        """Execute request to EDR Api to get detailed info for retry queue objects."""
        response = self.edrApiClient.get_subject_details(edr_id)
        if response.status_code != 200:
            raise Exception('Unsuccessful retry request to EDR.')
        return response

    def handle_status_response(self, response, tender_id):
        """Process unsuccessful request"""
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
        get_edr_id = spawn(self.get_edr_id)
        get_edr_details = spawn(self.get_edr_details)
        try:
            while not self.exit:
                gevent.sleep(self.delay)
                if get_edr_id.dead:
                    logger.warning("EDR handler worker get_edr_id dead try restart",
                                   extra=journal_context({"MESSAGE_ID": DATABRIDGE_RESTART_EDR_HANDLER_GET_ID}, {}))
                    get_edr_id = spawn(self.get_edr_id)
                    logger.info("EDR handler worker get_edr_id is up")
                if get_edr_details.dead:
                    logger.warning("EDR handler worker get_edr_details dead try restart",
                                   extra=journal_context({"MESSAGE_ID": DATABRIDGE_RESTART_EDR_HANDLER_GET_DETAILS}, {}))
                    get_edr_details = spawn(self.get_edr_details)
                    logger.info("EDR handler worker get_edr_id is up")
        except Exception as e:
            logger.error(e)
            get_edr_details.kill(timeout=5)
            get_edr_id.kill(timeout=5)

    def shutdown(self):
        self.exit = True
        logger.info('Worker EDR Handler complete his job.')
