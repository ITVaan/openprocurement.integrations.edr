# -*- coding: utf-8 -*-
import uuid
import unittest
import datetime
from gevent.queue import Queue
from gevent import sleep as gsleep
from openprocurement.integrations.edr.databridge.scanner import Scanner
from openprocurement.integrations.edr.databridge import scanner
from mock import patch, MagicMock
from time import sleep
from munch import munchify
from restkit.errors import (
    Unauthorized, RequestFailed, ResourceError
)


def custom_sleep(seconds):
    return gsleep(seconds=seconds)


class TestScannerWorker(unittest.TestCase):

    def test_init(self):
        worker = Scanner.spawn(None, None)
        self.assertGreater(datetime.datetime.now().isoformat(),
                           worker.start_time.isoformat())
        self.assertEqual(worker.tenders_sync_client, None)
        self.assertEqual(worker.filtered_tender_ids_queue, None)
        self.assertEqual(worker.delay, 15)
        self.assertEqual(worker.exit, False)

        worker.shutdown()
        del worker

    def test_worker(self):
        """ Try ... """
        tender_queue = Queue(10)
        client = MagicMock()
        client.sync_tenders.side_effect = [
            RequestFailed(),
            # worker must restart
            munchify({'prev_page': {'offset': '123'},
                      'next_page': {'offset': '1234'},
                      'data': [{'status': "active.qualification",
                                "id": uuid.uuid4().hex,
                                'procurementMethodType': 'aboveThresholdUA'}]}),
            Unauthorized(),
            munchify({'prev_page': {'offset': '123'},
                      'next_page': {'offset': '1234'},
                      'data': [{'status': "active.tendering",
                                "id": uuid.uuid4().hex,
                                'procurementMethodType': 'aboveThresholdUA'}]}),
            munchify({'prev_page': {'offset': '123'},
                      'next_page': {'offset': '1234'},
                      'data': [{'status': "active.pre-qualification",
                                "id": uuid.uuid4().hex,
                                'procurementMethodType': 'aboveThresholdEU'}]})
        ]

        worker = Scanner.spawn(client, tender_queue)
        sleep(40)

        # Kill worker
        worker.shutdown()
        del worker

        self.assertEqual(tender_queue.qsize(), 2)

    @patch('gevent.sleep')
    def test_425(self, gevent_sleep):
        gevent_sleep.side_effect = custom_sleep
        tender_queue = Queue(10)
        client = MagicMock()
        client.sync_tenders.side_effect = [
            ResourceError(http_code=425),
            munchify({'prev_page': {'offset': '123'},
                      'next_page': {'offset': '1234'},
                      'data': [{'status': "active.pre-qualification",
                                "id": uuid.uuid4().hex,
                                'procurementMethodType': 'aboveThresholdEU'}]})]

        worker = Scanner.spawn(client, tender_queue)
        sleep(10)
        # Kill worker
        worker.shutdown()
        del worker
        self.assertEqual(tender_queue.qsize(), 1)
        self.assertEqual(len(gevent_sleep.mock_calls), 3)
        self.assertEqual(scanner.sleep_multiplier, 1)

    @patch('gevent.sleep')
    def test_425_sleep_multiplier(self, gevent_sleep):
        gevent_sleep.side_effect = custom_sleep
        tender_queue = Queue(10)
        client = MagicMock()
        client.sync_tenders.side_effect = [
            ResourceError(http_code=425),
            ResourceError(http_code=425),
            ResourceError(http_code=425),
            munchify({'prev_page': {'offset': '123'},
                      'next_page': {'offset': '1234'},
                      'data': [{'status': "active.pre-qualification",
                                "id": uuid.uuid4().hex,
                                'procurementMethodType': 'aboveThresholdEU'}]})]

        worker = Scanner.spawn(client, tender_queue)
        sleep(25)
        self.assertEqual(tender_queue.qsize(), 1)
        self.assertEqual(len(gevent_sleep.mock_calls), 6)
        self.assertEqual(scanner.sleep_multiplier, 3)

        # Kill worker
        worker.shutdown()
        del worker





