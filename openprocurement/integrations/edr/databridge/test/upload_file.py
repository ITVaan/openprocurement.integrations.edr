import uuid
import unittest
import datetime
import requests_mock
from gevent.queue import Queue
from gevent import sleep
from openprocurement.integrations.edr.client import DocServiceClient
from openprocurement.integrations.edr.databridge.upload_file import UploadFile
from openprocurement_client.client import TendersClient
from mock import patch, MagicMock
from openprocurement.integrations.edr.databridge.utils import Data


def custom_sleep(seconds):
    return gsleep(seconds=0)


class TestUploadFileWorker(unittest.TestCase):
    tender_id = uuid.uuid4().hex
    award_id = uuid.uuid4().hex

    def test_init(self):
        worker = UploadFile.spawn(None, None, None, None, None)
        self.assertGreater(datetime.datetime.now().isoformat(),
                           worker.start_time.isoformat())

        self.assertEqual(worker.client, None)
        self.assertEqual(worker.upload_to_doc_service_queue, None)
        self.assertEqual(worker.upload_to_tender_queue, None)
        self.assertEqual(worker.processing_items, None)
        self.assertEqual(worker.doc_service_client, None)
        self.assertEqual(worker.delay, 15)
        self.assertEqual(worker.exit, False)

        worker.shutdown()
        self.assertEqual(worker.exit, True)
        del worker

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_doc_service_client(self, mrequest, gevent_sleep):
        gevent_sleep.side_effect = custom_sleep
        doc_service_client = DocServiceClient(host='127.0.0.1', port='80', token='')
        client = MagicMock()
        mrequest.get('{url}'.format(url=doc_service_client.url),
                     json={'data': {'url': 'http://docs-sandbox.openprocurement.org/get/8ccbfde0c6804143b119d9168452cb6f',
                                    'format': 'text/plain',
                                    'hash': 'md5:9a0364b9e99bb480dd25e1f0284c8555',
                                    'title': 'file.txt'}},
                     status_code=200)
        client.get_tender.side_effect = [{'data': {'id': uuid.uuid4().hex,
                                                   'documentOf': 'tender',
                                                   'documentType': 'registerExtract',
                                                   'url': 'url'}}]

        upload_to_doc_service_queue = Queue(10)
        upload_to_doc_service_queue.put(Data(TestUploadFileWorker.tender_id, TestUploadFileWorker.award_id, '123', 'awards', None, {'test_data': 'test_data'}))
        self.assertEqual(upload_to_doc_service_queue.qsize(), 1)
        processing_items = {TestUploadFileWorker.award_id: TestUploadFileWorker.tender_id}
        worker = UploadFile.spawn(MagicMock(), upload_to_doc_service_queue, MagicMock(), processing_items, doc_service_client)
        sleep(10)
        worker.shutdown()
        self.assertEqual(upload_to_doc_service_queue.qsize(), 0, 'Queue must be empty')
        self.assertEqual(mrequest.call_count, 2)
        self.assertEqual(mrequest.request_history[0].url, u'127.0.0.1:80/upload')
        self.assertItemsEqual(processing_items.keys(), [TestUploadFileWorker.award_id])