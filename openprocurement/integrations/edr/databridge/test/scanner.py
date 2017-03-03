# -*- coding: utf-8 -*-
import unittest
import datetime
from gevent import sleep
from gevent.queue import Queue
from openprocurement.integrations.edr.databridge.scanner import Scanner
from mock import patch, MagicMock
from time import sleep


def return_dict():
    return {'prev_page': '',
            'next_page': '',
            'data': [{'status': "active.qualification",
                      'procurementMethodType': 'aboveThresholdUA'}]}


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
        client.sync_tenders.side_effect = return_dict
        # client.sync_tenders.side_effect = [RequestFailed(), munchify({
        #     'session': {'headers': {'User-Agent': 'test.agent'}}
        # })]
        worker = Scanner.spawn(client, tender_queue)
        sleep(10)

        # Kill worker
        worker.shutdown()
        del worker

        self.assertNotEqual(tender_queue.qsize(), 0)




