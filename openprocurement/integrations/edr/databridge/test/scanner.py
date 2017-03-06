# -*- coding: utf-8 -*-
import uuid
import unittest
import datetime
from gevent import sleep
from gevent.queue import Queue
from openprocurement.integrations.edr.databridge.scanner import Scanner
from mock import patch, MagicMock
from time import sleep
from munch import munchify
from restkit.errors import (
    Unauthorized, RequestFailed
)


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

        self.assertEqual(tender_queue.qsize(), 3)




