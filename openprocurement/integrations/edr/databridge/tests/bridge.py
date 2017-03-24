# -*- coding: utf-8 -*-
from gevent import monkey
monkey.patch_all()

import unittest
import os
import uuid
import gevent

from gevent.pywsgi import WSGIServer
from bottle import Bottle, request, response
from mock import patch
from munch import munchify

from openprocurement.integrations.edr.databridge.bridge import EdrDataBridge
from openprocurement_client.client import TendersClientSync, TendersClient
from openprocurement.integrations.edr.client import DocServiceClient, ProxyClient
from openprocurement.integrations.edr.databridge.tests.utils import custom_sleep


config = {
    'main':
        {
            'tenders_api_server': 'http://127.0.0.1:20604',
            'tenders_api_version': '0',
            'public_tenders_api_server': 'http://127.0.0.1:20605',
            'doc_service_server': 'http://127.0.0.1',
            'doc_service_port': 20606,
            'doc_service_token': 'broker:broker',
            'proxy_server': 'http://127.0.0.1',
            'proxy_port': 20607,
            'proxy_token': 'cm9ib3Q6cm9ib3Q=',
            'buffers_size': 450,
            'full_stack_sync_delay': 15,
            'empty_stack_sync_delay': 101,
            'on_error_sleep_delay': 5,
            'api_token': "edrapi_secret"
        }
}

tender_id = uuid.uuid4().hex
award_id = uuid.uuid4().hex
edrpou = '14360570'
edr_id = '321'


class BaseServersTest(unittest.TestCase):
    """Api server to test openprocurement.integrations.edr.databridge.bridge """

    relative_to = os.path.dirname(__file__)  # crafty line

    @classmethod
    def setUpClass(cls):
        cls.api_server_bottle = Bottle()
        cls.proxy_server_bottle = Bottle()
        cls.doc_server_bottle = Bottle()
        cls.api_server = WSGIServer(('127.0.0.1', 20604), cls.api_server_bottle, log=None)
        cls.public_api_server = WSGIServer(('127.0.0.1', 20605), cls.api_server_bottle, log=None)
        cls.doc_server = WSGIServer(('127.0.0.1', 20606), cls.doc_server_bottle, log=None)
        cls.proxy_server = WSGIServer(('127.0.0.1', 20607), cls.proxy_server_bottle, log=None)

        # start servers
        cls.api_server.start()
        cls.public_api_server.start()
        cls.doc_server.start()
        cls.proxy_server.start()

    @classmethod
    def tearDownClass(cls):
        cls.api_server.close()
        cls.public_api_server.close()
        cls.doc_server.close()
        cls.proxy_server.close()

    def tearDown(self):
        pass


def setup_routing(app, func, path='/api/0/spore', method='GET'):
    app.route(path, method, func)


def response_spore():
    response.set_cookie("SERVER_ID", ("a7afc9b1fc79e640f2487ba48243ca071c07a823d27"
                                      "8cf9b7adf0fae467a524747e3c6c6973262130fac2b"
                                      "96a11693fa8bd38623e4daee121f60b4301aef012c"))
    return response


def sync_tenders():
    return munchify({'prev_page': {'offset': '123'},
                     'next_page': {'offset': '1234'},
                     'data': [{'status': "active.qualification",
                               'id': tender_id,
                               'procurementMethodType': 'aboveThresholdUA'}]})


def get_tender():
    return munchify(
                {'prev_page': {'offset': '123'},
                 'next_page': {'offset': '1234'},
                 'data': {
                     'status': "active.pre-qualification",
                     'id': tender_id,
                     'procurementMethodType': 'aboveThresholdEU',
                     'awards': [
                         {'id': award_id,
                          'status': 'pending',
                          'suppliers': [
                              {'identifier': {'scheme': 'UA-EDR',
                                              'id': edrpou}
                               }]
                          }
                      ]
                  }})


def verify():
    return munchify({'data': [{'id': '321'}]})


def details():
    return munchify({'data': {}})


def upload_doc_service():
    return munchify({'data': {'url': 'http://docs-sandbox.openprocurement.org/get/8ccbfde0c6804143b119d9168452cb6f',
                              'format': 'application/yaml',
                              'hash': 'md5:9a0364b9e99bb480dd25e1f0284c8555',
                              'title': 'edr_request.yaml'}})


def upload_api():
    response.status = 201
    return munchify({'data': {'id': uuid.uuid4().hex,
                              'documentOf': 'tender',
                              'documentType': 'registerExtract',
                              'url': 'url'}})


class TestBridgeWorker(BaseServersTest):

    def test_init(self):
        setup_routing(self.api_server_bottle, response_spore)
        worker = EdrDataBridge(config)
        self.assertEqual(worker.delay, 15)
        self.assertEqual(worker.increment_step, 1)
        self.assertEqual(worker.decrement_step, 1)

        # check clients
        self.assertTrue(isinstance(worker.tenders_sync_client, TendersClientSync))
        self.assertTrue(isinstance(worker.client, TendersClient))
        self.assertTrue(isinstance(worker.proxyClient, ProxyClient))
        self.assertTrue(isinstance(worker.doc_service_client, DocServiceClient))

        # check events
        self.assertFalse(worker.initialization_event.is_set())
        self.assertTrue(worker.until_too_many_requests_event.is_set())

        # check processing items
        self.assertEqual(worker.processing_items, {})

        del worker

    def test_run(self):
        setup_routing(self.api_server_bottle, response_spore)
        setup_routing(self.api_server_bottle, sync_tenders, path='/api/0/tenders')
        setup_routing(self.api_server_bottle, get_tender, path='/api/0/tenders/{}'.format(tender_id))
        setup_routing(self.proxy_server_bottle, verify, path='/verify')
        setup_routing(self.proxy_server_bottle, details, path='/details/{}'.format(edr_id))
        setup_routing(self.doc_server_bottle, upload_doc_service, path='/upload', method='POST')
        setup_routing(self.api_server_bottle, upload_api,
                      path='/api/0/tenders/{}/awards/{}/documents'.format(tender_id, award_id), method='POST')
        with gevent.Timeout(2, False):
            worker = EdrDataBridge(config).run()
            self.assertIsNotNone(worker.jobs)
            worker.shutdown()
            self.assertEqual(worker.exit, True)
            del worker
