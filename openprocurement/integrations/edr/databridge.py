from gevent import monkey
monkey.patch_all()

try:
    import urllib3.contrib.pyopenssl
    urllib3.contrib.pyopenssl.inject_into_urllib3()
except ImportError:
    pass

import logging
import logging.config
import os
import argparse
import json
import gevent

from uuid import uuid4
import io
from retrying import retry
from yaml import load
from gevent.queue import Queue
from collections import namedtuple

from openprocurement_client.client import TendersClientSync, TendersClient
from openprocurement.integrations.edr.client import EdrClient
from openprocurement.integrations.edr.journal_msg_ids import (
    DATABRIDGE_INFO, DATABRIDGE_SYNC_SLEEP, DATABRIDGE_GET_TENDER_FROM_QUEUE, DATABRIDGE_TENDER_PROCESS,
    DATABRIDGE_EMPTY_RESPONSE, DATABRIDGE_WORKER_DIED, DATABRIDGE_RESTART, DATABRIDGE_START,
    DATABRIDGE_UNAUTHORIZED_EDR, DATABRIDGE_SUCCESS_CREATE_FILE, DATABRIDGE_SUCCESS_UPLOAD_FILE)

logger = logging.getLogger("openprocurement.integrations.edr.databridge")
Data = namedtuple('Data', ['tender_id', 'item_id', 'code', 'item_name', 'file_content'])
id_passport_len = 9

def generate_req_id():
    return b'edr-api-data-bridge-req-' + str(uuid4()).encode('ascii')


def journal_context(record={}, params={}):
    for k, v in params.items():
        record["JOURNAL_" + k] = v
    return record


def create_file(details):
    temporary_file = io.BytesIO()
    temporary_file.name = 'edr_request.json'
    temporary_file.write(json.dumps(details, indent=4, separators=(',', ': '), ensure_ascii=False).encode('utf-8'))
    temporary_file.seek(0)

    return temporary_file


def validate_param(code):
    return 'code' if code.isdigit() and len(code) != id_passport_len else 'passport'


class EdrDataBridge(object):
    """ Edr API Data Bridge """

    pre_qualification_procurementMethodType = ('aboveThresholdEU', 'competitiveDialogueUA', 'competitiveDialogueEU')
    qualification_procurementMethodType = ('aboveThresholdUA', 'aboveThresholdUA.defense', 'aboveThresholdEU', 'competitiveDialogueUA.stage2', 'competitiveDialogueEU.stage2')

    def __init__(self, config):
        super(EdrDataBridge, self).__init__()
        self.config = config

        api_server = self.config_get('tenders_api_server')
        api_version = self.config_get('tenders_api_version')
        ro_api_server = self.config_get('public_tenders_api_server') or api_server
        buffers_size = self.config_get('buffers_size') or 500

        self.tenders_sync_client = TendersClientSync('', host_url=ro_api_server, api_version=api_version)
        self.client = TendersClient(self.config_get('api_token'), host_url=api_server, api_version=api_version)
        self.filtered_tenders_queue = Queue(maxsize=buffers_size)
        self.data_queue = Queue(maxsize=buffers_size)
        self.subjects_queue = Queue(maxsize=buffers_size)
        self.upload_file_queue = Queue(maxsize=buffers_size)
        self.update_file_queue = Queue(maxsize=buffers_size)
        self.initialization_event = gevent.event.Event()
        self.until_too_many_requests_event = gevent.event.Event()
        self.until_too_many_requests_event.set()
        self.delay = self.config_get('delay') or 15
        self.edrApiClient = EdrClient(host=self.config_get('edr_api_server'), token=self.config_get('edr_api_token'), port=self.config_get('edr_api_port'))
        self.required_fields = ['names', 'founders', 'management', 'activity_kinds', 'address', 'bankruptcy']

    def config_get(self, name):
        return self.config.get('main').get(name)

    @retry(stop_max_attempt_number=5, wait_exponential_multiplier=1000)
    def initialize_sync(self, params=None, direction=None):
        self.initialization_event.clear()
        if direction == "backward":
            assert params['descending']
            response = self.tenders_sync_client.sync_tenders(params,
                                                             extra_headers={'X-Client-Request-ID': generate_req_id()})
            # set values in reverse order due to 'descending' option
            self.initial_sync_point = {'forward_offset': response.prev_page.offset,
                                       'backward_offset': response.next_page.offset}
            self.initialization_event.set()  # wake up forward worker
            logger.info("Initial sync point {}".format(self.initial_sync_point))
            return response
        else:
            assert 'descending' not in params
            gevent.wait([self.initialization_event])
            params['offset'] = self.initial_sync_point['forward_offset']
            logger.info("Starting forward sync from offset {}".format(params['offset']))
            return self.tenders_sync_client.sync_tenders(params,
                                                         extra_headers={'X-Client-Request-ID': generate_req_id()})

    def get_tenders(self, params={}, direction=""):
        response = self.initialize_sync(params=params, direction=direction)

        while not (params.get('descending') and
                   not len(response.data) and
                   params.get('offset') == response.next_page.offset):
            tenders = response.data if response else []
            params['offset'] = response.next_page.offset
            for tender in tenders:
                if (tender['status'] == "active.qualification" and
                    tender['procurementMethodType'] in self.qualification_procurementMethodType) \
                    or (tender['status'] == 'active.pre-qualification' and
                        tender['procurementMethodType'] in self.pre_qualification_procurementMethodType):
                    yield tender
                else:
                    logger.info('Skipping tender {} with status {} with procurementMethodType {}'.format(
                                    tender['id'], tender['status'], tender['procurementMethodType']),
                                extra=journal_context({"MESSAGE_ID": DATABRIDGE_INFO},
                                                      params={"TENDER_ID": tender['id']}))
            logger.info('Sleep {} sync...'.format(direction), extra=journal_context({"MESSAGE_ID": DATABRIDGE_SYNC_SLEEP}))
            gevent.sleep(self.delay)
            response = self.tenders_sync_client.sync_tenders(params,
                                                             extra_headers={'X-Client-Request-ID': generate_req_id()})

    def prepare_data(self):
        while True:
            tender_id = self.filtered_tenders_queue.get()
            try:
                tender = self.tenders_sync_client.get_tender(
                                            tender_id, extra_headers={'X-Client-Request-ID': generate_req_id()})['data']
                logger.info('Get tender {} from filtered_tenders_queue'.format(tender_id),
                            extra=journal_context({"MESSAGE_ID": DATABRIDGE_GET_TENDER_FROM_QUEUE},
                            params={"TENDER_ID": tender['id']}))
            except Exception as e:
                logger.warn('Fail to get tender info {}'.format(tender_id),
                            extra=journal_context(params={"TENDER_ID": tender['id']}))
                logger.exception(e)
                logger.info('Put tender {} back to tenders queue'.format(tender_id),
                            extra=journal_context(params={"TENDER_ID": tender['id']}))
                self.filtered_tenders_queue.put(tender_id)
            else:
                if 'awards' in tender:
                    for award in tender['awards']:
                        logger.info('Processing tender {} award {}'.format(tender['id'], award['id']),
                                    extra=journal_context({"MESSAGE_ID": DATABRIDGE_TENDER_PROCESS},
                                    params={"TENDER_ID": tender['id']}))
                        if award['status'] == 'pending' and not [document for document in award.get('documents', [])
                                                                 if document.get('documentType') == 'registerExtract']:
                            for supplier in award['suppliers']:
                                tender_data = Data(tender['id'], award['id'], supplier['identifier']['id'], 'awards', None)
                                self.data_queue.put(tender_data)
                        else:
                            logger.info('Tender {} award {} is not in status pending or award has already document '
                                        'with documentType registerExtract.'.format(tender_id, award['id']),
                                        extra=journal_context(params={"TENDER_ID": tender['id']}))
                elif 'bids' in tender:
                    for qualification in tender['qualifications']:
                        if qualification['status'] == 'pending' and \
                                not [document for document in qualification.get('documents', [])
                                     if document.get('documentType') == 'registerExtract']:
                            appropriate_bid = [b for b in tender['bids'] if b['id'] == qualification['bidID']][0]
                            tender_data = Data(tender['id'], qualification['id'],
                                               appropriate_bid['tenderers'][0]['identifier']['id'], 'qualifications', None)
                            self.data_queue.put(tender_data)
                            logger.info('Processing tender {} bid {}'.format(tender['id'], appropriate_bid['id']),
                                        extra=journal_context({"MESSAGE_ID": DATABRIDGE_TENDER_PROCESS},
                                                              params={"TENDER_ID": tender['id']}))
                        else:
                            logger.info('Tender {} qualification {} is not in status pending or qualification has '
                                        'already document with documentType registerExtract.'.format(
                                            tender_id, qualification['id']),
                                            extra=journal_context(params={"TENDER_ID": tender['id']}))

    def get_subject_id(self):
        while True:
            try:
                tender_data = self.data_queue.get()
                logger.info('Get tender {} from data_queue'.format(tender_data.tender_id),
                            extra=journal_context({"MESSAGE_ID": DATABRIDGE_GET_TENDER_FROM_QUEUE},
                                                  params={"TENDER_ID": tender_data.tender_id}))
            except Exception as e:
                logger.warn('Fail to get tender {} with {} id {} from edrpou queue'.format(
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
                logger.warn('Fail to get tender {} with {} id {} from edrpou queue'.format(
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

    def upload_file(self):
        while True:
            tender_data = self.upload_file_queue.get()
            try:
                tender = self.tenders_sync_client.get_tender(
                    tender_data.tender_id, extra_headers={'X-Client-Request-ID': generate_req_id()})
                # create patch request to award/qualification with document to upload
                if tender_data.item_name == 'awards':
                    document = self.client.upload_award_document(create_file(tender_data.file_content), tender, tender_data.item_id)
                else:
                    document = self.client.upload_qualification_document(create_file(tender_data.file_content), tender, tender_data.item_id)
            except Exception as e:
                logger.info('Exception while uploading file to tender {} {} {}. Message: {}'.format(
                    tender_data.tender_id, tender_data.item_name, tender_data.item_id, e.message))
                raise e
            else:
                data = Data(tender_data.tender_id, tender_data.item_id,
                            tender_data.code, tender_data.item_name, {'document_id': document['data']['id']})
                self.update_file_queue.put(data)
                logger.info('Successfully uploaded file for tender {} {} {}'.format(
                        tender_data.tender_id, tender_data.item_name, tender_data.item_id),
                    extra=journal_context({"MESSAGE_ID": DATABRIDGE_SUCCESS_UPLOAD_FILE},
                                          params={"TENDER_ID": tender_data.tender_id}))

    def update_file(self):
        while True:
            tender_data = self.update_file_queue.get()
            try:
                tender = self.tenders_sync_client.get_tender(
                    tender_data.tender_id, extra_headers={'X-Client-Request-ID': generate_req_id()})
                self.client._patch_resource_item('{}/{}/{}/{}/documents/{}'.format(
                    self.client.prefix_path, tender_data.tender_id, tender_data.item_name, tender_data.item_id,
                    tender_data.file_content['document_id']), payload={"data": {"documentType": "registerExtract"}},
                                                              headers={'X-Access-Token': getattr(getattr(tender, 'access', ''), 'token', '')})
            except Exception as e:
                logger.info('Exception while updating file to tender {} {} {}. Message: {}'.format(
                                tender_data.tender_id, tender_data.item_name, tender_data.item_id, e.message))
                raise e
            else:
                logger.info('Successfully updated file for tender {} {} {}'.format(
                        tender_data.tender_id, tender_data.item_name, tender_data.item_id),
                    extra=journal_context({"MESSAGE_ID": DATABRIDGE_SUCCESS_UPLOAD_FILE},
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

    def get_tenders_forward(self):
        logger.info('Start forward data sync worker...')
        params = {'opt_fields': 'status,procurementMethodType', 'mode': '_all_'}
        try:
            for tender in self.get_tenders(params=params, direction="forward"):
                logger.info('Forward sync: Put tender {} to process...'.format(tender['id']),
                            extra=journal_context({"MESSAGE_ID": DATABRIDGE_TENDER_PROCESS},
                                                  {"TENDER_ID": tender['id']}))
                self.filtered_tenders_queue.put(tender['id'])
        except Exception as e:
            logger.warning('Forward worker died!', extra=journal_context({"MESSAGE_ID": DATABRIDGE_WORKER_DIED}, {}))
            logger.exception(e)
        else:
            logger.warning('Forward data sync finished!')

    def get_tenders_backward(self):
        logger.info('Start backward data sync worker...')
        params = {'opt_fields': 'status,procurementMethodType', 'descending': 1, 'mode': '_all_'}
        try:
            for tender in self.get_tenders(params=params, direction="backward"):
                logger.info('Backward sync: Put tender {} to process...'.format(tender['id']),
                            extra=journal_context({"MESSAGE_ID": DATABRIDGE_TENDER_PROCESS},
                                                  {"TENDER_ID": tender['id']}))
                self.filtered_tenders_queue.put(tender['id'])
        except Exception as e:
            logger.warning('Backward worker died!', extra=journal_context({"MESSAGE_ID": DATABRIDGE_WORKER_DIED}, {}))
            logger.exception(e)
        else:
            logger.info('Backward data sync finished.')

    def _start_synchronization_workers(self):
        logger.info('Starting forward and backward sync workers')
        self.jobs = [gevent.spawn(self.get_tenders_backward),
                     gevent.spawn(self.get_tenders_forward)]

    def _restart_synchronization_workers(self):
        logger.warning('Restarting synchronization', extra=journal_context({"MESSAGE_ID": DATABRIDGE_RESTART}, {}))
        for j in self.jobs:
            j.kill()
        self._start_synchronization_workers()

    def _start_steps(self):
        self.immortal_jobs = {'prepare_data': gevent.spawn(self.prepare_data),
                              'get_subject_id': gevent.spawn(self.get_subject_id),
                              'get_subject_details': gevent.spawn(self.get_subject_details),
                              'upload_file': gevent.spawn(self.upload_file),
                              'update_file': gevent.spawn(self.update_file)}

    def run(self):
        logger.info('Start EDR API Data Bridge', extra=journal_context({"MESSAGE_ID": DATABRIDGE_START}, {}))
        self._start_synchronization_workers()
        self._start_steps()
        backward_worker, forward_worker = self.jobs
        counter = 0

        try:
            while True:
                gevent.sleep(self.delay)
                if counter == 20:
                    logger.info(
                        'Current state: filtered tenders {}; Data queue {}; Subjects {}; Upload file {}; '
                        'Update file {}'.format(self.filtered_tenders_queue.qsize(), self.data_queue.qsize(),
                                                self.subjects_queue.qsize(), self.upload_file_queue.qsize(),
                                                self.update_file_queue.qsize()))
                    counter = 0
                counter += 1
                if forward_worker.dead or (backward_worker.dead and not backward_worker.successful()):
                    self._restart_synchronization_workers()
                    backward_worker, forward_worker = self.jobs

                for name, job in self.immortal_jobs.items():
                    if job.dead:
                        logger.warning('Restarting {} worker'.format(name))
                        self.immortal_jobs[name] = gevent.spawn(getattr(self, name))
        except KeyboardInterrupt:
            logger.info('Exiting...')
            gevent.killall(self.jobs, timeout=5)
            gevent.killall(self.immortal_jobs, timeout=5)
        except Exception as e:
            logger.error(e)


def main():
    parser = argparse.ArgumentParser(description='Edr API Data Bridge')
    parser.add_argument('config', type=str, help='Path to configuration file')
    parser.add_argument('--tender', type=str, help='Tender id to sync', dest="tender_id")
    params = parser.parse_args()
    if os.path.isfile(params.config):
        with open(params.config) as config_file_obj:
            config = load(config_file_obj.read())
        logging.config.dictConfig(config)
        EdrDataBridge(config).run()
    else:
        logger.info('Invalid configuration file. Exiting...')


if __name__ == "__main__":
    main()
