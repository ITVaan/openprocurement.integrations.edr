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

from openprocurement.integrations.edr.databridge.journal_msg_ids import (
    DATABRIDGE_START, DATABRIDGE_SUCCESS_UPLOAD_FILE)
from openprocurement.integrations.edr.databridge.utils import journal_context, generate_req_id, Data, create_file

logger = logging.getLogger("openprocurement.integrations.edr.databridge")


class UploadFile(object):
    """ Upload file with details """

    pre_qualification_procurementMethodType = ('aboveThresholdEU', 'competitiveDialogueUA', 'competitiveDialogueEU')
    qualification_procurementMethodType = ('aboveThresholdUA', 'aboveThresholdUA.defense', 'aboveThresholdEU', 'competitiveDialogueUA.stage2', 'competitiveDialogueEU.stage2')
    required_fields = ['names', 'founders', 'management', 'activity_kinds', 'address', 'bankruptcy']

    def __init__(self, tenders_sync_client, client, upload_file_queue, update_file_queue, delay=15):
        super(UploadFile, self).__init__()
        self.delay = delay

        # init clients
        self.tenders_sync_client = tenders_sync_client
        self.client = client

        # init queues for workers
        self.upload_file_queue = upload_file_queue
        self.update_file_queue = update_file_queue

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

    def run(self):
        logger.info('Start UploadFile worker', extra=journal_context({"MESSAGE_ID": DATABRIDGE_START}, {}))
        upload_file = gevent.spawn(self.upload_file)
        update_file = gevent.spawn(self.update_file)
        try:
            while True:
                gevent.sleep(self.delay)
                if upload_file.dead:
                    logger.warning("UploadFile worker upload_file dead try restart")
                    upload_file = gevent.spawn(self.upload_file)
                    logger.info("UploadFile worker get_subject_id is up")
                if update_file.dead:
                    logger.warning("UploadFile worker update_file dead try restart")
                    update_file = gevent.spawn(self.update_file)
                    logger.info("UploadFile worker get_subject_id is up")
        except Exception as e:
            logger.error(e)
            upload_file.kill(timeout=5)
            update_file.kill(timeout=5)
