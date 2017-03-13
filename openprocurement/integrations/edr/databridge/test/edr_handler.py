# -*- coding: utf-8 -*-
import uuid
import unittest
import datetime
from gevent.queue import Queue
from gevent import sleep as gsleep
from openprocurement.integrations.edr.databridge.edr_handler import EdrHandler
# from openprocurement.integrations.edr.databridge.test.scanner import custom_sleep
from openprocurement.integrations.edr.databridge.utils import Data
from mock import patch, MagicMock
import requests_mock
from time import sleep
from munch import munchify
from restkit.errors import (
    Unauthorized, RequestFailed, ResourceError
)

from openprocurement.integrations.edr.client import ProxyClient

def custom_sleep(seconds):
    return gsleep(seconds=1)


class TestEdrHandlerWorker(unittest.TestCase):

    def test_init(self):
        worker = EdrHandler.spawn(None, None, None, None)
        self.assertGreater(datetime.datetime.now().isoformat(),
                           worker.start_time.isoformat())

        self.assertEqual(worker.proxyClient, None)
        self.assertEqual(worker.edrpou_codes_queue, None)
        self.assertEqual(worker.edr_ids_queue, None)
        self.assertEqual(worker.upload_to_doc_service_queue, None)
        self.assertEqual(worker.delay, 15)
        self.assertEqual(worker.exit, False)

        worker.shutdown()
        self.assertEqual(worker.exit, True)
        del worker

    @requests_mock.Mocker()
    @patch('gevent.sleep')
    def test_worker_up(self, mrequest, gevent_sleep):
        gevent_sleep.side_effect = custom_sleep
        proxy_client = ProxyClient(host='127.0.0.1', port='80', token='')
        mrequest.get("{uri}".format(uri=proxy_client.verify_url),
                     [{'json': [{'id': '123'}], 'status_code': 200},
                      {'json': [{'id': '321'}], 'status_code': 200}])

        edrpou_codes_queue = Queue(10)
        edrpou_codes_queue.put(Data(uuid.uuid4().hex, 'award_id', '123', "awards", None, None))
        edrpou_codes_queue.put(Data(uuid.uuid4().hex, 'award_id', '135', "awards", None, None))

        worker = EdrHandler.spawn(proxy_client, edrpou_codes_queue, MagicMock(), MagicMock())

        sleep(25)

        worker.shutdown()
        self.assertEqual(mrequest.call_count, 2)  # Requests must call proxy two times
        self.assertEqual(mrequest.request_history[0].url,
                         u'127.0.0.1:80/verify?code=123')
        self.assertEqual(mrequest.request_history[1].url,
                         u'127.0.0.1:80/verify?code=135')


