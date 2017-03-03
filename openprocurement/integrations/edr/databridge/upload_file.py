# -*- coding: utf-8 -*-
from gevent import monkey
from munch import munchify
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
    DATABRIDGE_SUCCESS_UPLOAD_FILE, DATABRIDGE_UNSUCCESS_UPLOAD_FILE,
    DATABRIDGE_UNSUCCESS_RETRY_UPLOAD_FILE, DATABRIDGE_SUCCESS_UPDATE_FILE, DATABRIDGE_UNSUCCESS_UPDATE_FILE,
    DATABRIDGE_UNSUCCESS_RETRY_UPDATE_FILE, DATABRIDGE_START_UPLOAD)
from openprocurement.integrations.edr.databridge.utils import journal_context, Data, create_file

logger = logging.getLogger(__name__)


class UploadFile(Greenlet):
    """ Upload file with details """

    pre_qualification_procurementMethodType = ('aboveThresholdEU', 'competitiveDialogueUA', 'competitiveDialogueEU')
    qualification_procurementMethodType = ('aboveThresholdUA', 'aboveThresholdUA.defense', 'aboveThresholdEU', 'competitiveDialogueUA.stage2', 'competitiveDialogueEU.stage2')

    def __init__(self, client, upload_file_queue, update_file_queue, processing_items, delay=15):
        super(UploadFile, self).__init__()
        self.exit = False
        self.start_time = datetime.now()

        self.delay = delay
        self.processing_items = processing_items

        # init clients
        self.client = client

        # init queues for workers
        self.upload_file_queue = upload_file_queue
        self.update_file_queue = update_file_queue

        # retry queues for workers
        self.retry_upload_file_queue = Queue(maxsize=500)
        self.retry_update_file_queue = Queue(maxsize=500)

    def upload_file(self):
        """Get data from upload_file_queue; Create file of the Data.file_content data; If upload successful put Data
        object to update_file_queue, otherwise put Data to retry_upload_file_queue."""
        while True:
            tender_data = self.upload_file_queue.get()
            try:
                raise Exception
                # create patch request to award/qualification with document to upload
                if tender_data.item_name == 'awards':
                    document = self.client.upload_award_document(create_file(tender_data.file_content),
                                                                 munchify({'data': {'id': tender_data.tender_id}}),
                                                                 tender_data.item_id)
                else:
                    document = self.client.upload_qualification_document(create_file(tender_data.file_content),
                                                                         munchify({'data': {'id': tender_data.tender_id}}),
                                                                         tender_data.item_id)
            except Exception as e:
                logger.info('Exception while uploading file to tender {} {} {}. Message: {}'.format(
                    tender_data.tender_id, tender_data.item_name, tender_data.item_id, e.message),
                    extra=journal_context({"MESSAGE_ID": DATABRIDGE_UNSUCCESS_UPLOAD_FILE},
                                            params={"TENDER_ID": tender_data.tender_id,
                                                    "ITEM_ID": tender_data.item_id}))
                logger.exception(e)
                self.retry_upload_file_queue.put(tender_data)
            else:
                data = Data(tender_data.tender_id, tender_data.item_id, tender_data.code,
                            tender_data.item_name, tender_data.edr_ids, {'document_id': document['data']['id']})
                self.update_file_queue.put(data)
                logger.info('Successfully uploaded file for tender {} {} {}'.format(
                        tender_data.tender_id, tender_data.item_name, tender_data.item_id),
                    extra=journal_context({"MESSAGE_ID": DATABRIDGE_SUCCESS_UPLOAD_FILE},
                                          params={"TENDER_ID": tender_data.tender_id}))

    def retry_upload_file(self):
        """Get data from retry_upload_file_queue; If upload were successful put Data obj to update_file_queue, otherwise
         put Data obj back to retry_upload_file_queue"""
        while True:
            tender_data = self.retry_upload_file_queue.get()
            try:
                # create patch request to award/qualification with document to upload
                document = self.client_upload_file(tender_data)
            except Exception as e:
                logger.info('Exception while retry uploading file to tender {} {} {}. Message: {}'.format(
                    tender_data.tender_id, tender_data.item_name, tender_data.item_id, e.message),
                    extra=journal_context({"MESSAGE_ID": DATABRIDGE_UNSUCCESS_RETRY_UPLOAD_FILE},
                                            params={"TENDER_ID": tender_data.tender_id,
                                                    "ITEM_ID": tender_data.item_id}))
                self.retry_upload_file_queue.put(tender_data)
            else:
                data = Data(tender_data.tender_id, tender_data.item_id, tender_data.code,
                            tender_data.item_name, tender_data.edr_ids, {'document_id': document['data']['id']})
                self.update_file_queue.put(data)
                logger.info('Successfully uploaded file for tender {} {} {}'.format(
                        tender_data.tender_id, tender_data.item_name, tender_data.item_id),
                    extra=journal_context({"MESSAGE_ID": DATABRIDGE_SUCCESS_UPLOAD_FILE},
                                          params={"TENDER_ID": tender_data.tender_id}))

    @retry(stop_max_attempt_number=5, wait_exponential_multiplier=1000)
    def client_upload_file(self, tender_data):
        """Process upload request for retry queue objects."""
        if tender_data.item_name == 'awards':
            document = self.client.upload_award_document(create_file(tender_data.file_content),
                                                         munchify({'data': {'id': tender_data.tender_id}}),
                                                         tender_data.item_id)
        else:
            document = self.client.upload_qualification_document(create_file(tender_data.file_content),
                                                                 munchify({'data': {'id': tender_data.tender_id}}),
                                                                 tender_data.item_id)
        return document

    def update_file(self):
        """Get data from update_file_queue; Update field documentType for tender_data.file_content['document_id'];
        If update were unsuccessful put Data object to retry_update_file_queue, otherwise delete given
        award/qualification from processing_items."""
        while True:
            tender_data = self.update_file_queue.get()
            try:
                self.client._patch_resource_item('{}/{}/{}/{}/documents/{}'.format(
                    self.client.prefix_path, tender_data.tender_id, tender_data.item_name, tender_data.item_id,
                    tender_data.file_content['document_id']), payload={"data": {"documentType": "registerExtract"}})
            except Exception as e:
                logger.info('Exception while updating file to tender {} {} {}. Message: {}'.format(
                                tender_data.tender_id, tender_data.item_name, tender_data.item_id, e.message),
                    extra=journal_context({"MESSAGE_ID": DATABRIDGE_UNSUCCESS_UPDATE_FILE},
                                          params={"TENDER_ID": tender_data.tender_id}))
                self.retry_update_file_queue.put(tender_data)
            else:
                logger.info('Successfully updated file for tender {} {} {}'.format(
                        tender_data.tender_id, tender_data.item_name, tender_data.item_id),
                    extra=journal_context({"MESSAGE_ID": DATABRIDGE_SUCCESS_UPDATE_FILE},
                                          params={"TENDER_ID": tender_data.tender_id}))
                # delete current tender after successful upload/update file ( to avoid reloading file)
                del self.processing_items[tender_data.item_id]

    def retry_update_file(self):
        """Get data from retry_update_file_queue; If update was unsuccessful put Data obj back to retry_update_file_queue"""
        while True:
            tender_data = self.retry_update_file_queue.get()
            try:
                self.client_update_file(tender_data)
            except Exception as e:
                logger.info('Exception while retry updating file to tender {} {} {}. Message: {}'.format(
                                tender_data.tender_id, tender_data.item_name, tender_data.item_id, e.message),
                    extra=journal_context({"MESSAGE_ID": DATABRIDGE_UNSUCCESS_RETRY_UPDATE_FILE},
                                          params={"TENDER_ID": tender_data.tender_id}))
                logger.exception(e)
                self.retry_update_file_queue.put(tender_data)
            else:
                logger.info('Successfully updated file for tender {} {} {}'.format(
                        tender_data.tender_id, tender_data.item_name, tender_data.item_id),
                    extra=journal_context({"MESSAGE_ID": DATABRIDGE_SUCCESS_UPDATE_FILE},
                                          params={"TENDER_ID": tender_data.tender_id}))
                # delete current tender after successful upload/update file ( to avoid reloading file)
                del self.processing_items[tender_data.item_id]

    @retry(stop_max_attempt_number=5, wait_exponential_multiplier=1000)
    def client_update_file(self, tender_data):
        """Process update request for retry queue objects."""
        self.client._patch_resource_item('{}/{}/{}/{}/documents/{}'.format(
            self.client.prefix_path, tender_data.tender_id, tender_data.item_name, tender_data.item_id,
            tender_data.file_content['document_id']), payload={"data": {"documentType": "registerExtract"}})

    def run(self):
        logger.info('Start UploadFile worker', extra=journal_context({"MESSAGE_ID": DATABRIDGE_START_UPLOAD}, {}))
        self.immortal_jobs = {'upload_file': spawn(self.upload_file),
                              'update_file': spawn(self.update_file),
                              'retry_upload_file': spawn(self.retry_upload_file),
                              'retry_update_file': spawn(self.retry_update_file)}

        try:
            while not self.exit:
                gevent.sleep(self.delay)
                for name, job in self.immortal_jobs.items():
                    if job.dead:
                        logger.warning("{} worker dead try restart".format(name), extra=journal_context({"MESSAGE_ID": 'DATABRIDGE_RESTART_{}'.format(name.lower())}, {}))
                        self.immortal_jobs[name] = gevent.spawn(getattr(self, name))
                        logger.info("{} worker get_edr_id is up".format(name))

        except Exception as e:
            logger.error(e)
            gevent.killall(self.immortal_jobs.values(), timeout=5)

    def shutdown(self):
        self.exit = True
        logger.info('Worker UploadFile complete his job.')
