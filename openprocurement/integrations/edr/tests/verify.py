# -*- coding: utf-8 -*-
import webtest
import os

from openprocurement.integrations.edr.tests.base import BaseWebTest
from openprocurement.integrations.edr.tests._server import (setup_routing, response_code, response_passport,
    check_headers, payment_required, forbidden, not_acceptable, too_many_requests, two_error_messages, bad_gateway,
    server_error, response_details, too_many_requests_details, bad_gateway_details)


class TestVerify(BaseWebTest):
    """ Test verify view """

    def test_opt_json(self):
        setup_routing(self.edr_api_app, func=response_code)
        response = self.app.get('/verify?code=14360570&opt_jsonp=callback')
        self.assertEqual(response.status, '200 OK')
        self.assertEqual(response.content_type, 'application/javascript')
        self.assertNotIn('{\n    "', response.body)
        self.assertIn('callback({', response.body)

    def test_pretty_json(self):
        setup_routing(self.edr_api_app, func=response_code)
        response = self.app.get('/verify?code=14360570&opt_jsonp=callback&opt_pretty=1')
        self.assertEqual(response.status, '200 OK')
        self.assertEqual(response.content_type, 'application/javascript')
        self.assertIn('{\n    "', response.body)
        self.assertIn('callback({', response.body)

    def test_permission_deny(self):
        old = self.app.authorization
        self.app.authorization = ('Basic', ('', ''))
        setup_routing(self.edr_api_app, func=response_code)
        response = self.app.get('/verify?code=14360570', status=403)
        self.assertEqual(response.status, '403 Forbidden')
        self.assertEqual(response.content_type, 'application/json')
        self.assertEqual(response.json['errors'],
                         [{
                             "location": "url",
                             "name": "permission",
                             "description": "Forbidden"
                         }]
        )
        self.app.authorization = old

    def test_edrpou(self):
        """ Get info by custom edrpou """
        setup_routing(self.edr_api_app, func=response_code)
        response = self.app.get('/verify?code=14360570')
        self.assertEqual(response.status, '200 OK')
        self.assertEqual(response.content_type, 'application/json')
        self.assertEqual(
            response.json['data'],
            [{u'state': {u'code': 1,
                        u'description': u'https://zqedr-api.nais.gov.ua/1.0/subjects/2842335'},
             u'identification': {u'url': u'https://zqedr-api.nais.gov.ua/1.0/subjects/2842335',
                                 u'schema': u'UA-EDR',
                                 u'id': u'14360570',
                                 u'legalName': u"АКЦІОНЕРНЕ ТОВАРИСТВО КОМЕРЦІЙНИЙ БАНК \"ПРИВАТБАНК\""},
             u'id': 2842335}])

    def test_passport(self):
        """ Get info by passport number """
        setup_routing(self.edr_api_app, func=response_passport)
        response = self.app.get('/verify?passport=СН012345')
        self.assertEqual(response.status, '200 OK')
        self.assertEqual(response.content_type, 'application/json')
        self.assertEqual(
            response.json['data'],
            [{u'state': {u'code': 1,
                        u'description': u'https://zqedr-api.nais.gov.ua/1.0/subjects/2842336'},
             u'identification': {u'url': u'https://zqedr-api.nais.gov.ua/1.0/subjects/2842336',
                                 u'schema': u'UA-EDR',
                                 u'id': u'СН012345',
                                 u'legalName': u'СН012345'},
             u'id': 2842336}])

    def test_new_passport(self):
        """ Get info by new passport number with 13-digits"""
        setup_routing(self.edr_api_app, func=response_passport)
        response = self.app.get('/verify?passport=123456789')
        self.assertEqual(response.status, '200 OK')
        self.assertEqual(response.content_type, 'application/json')
        self.assertEqual(
            response.json['data'],
            [{u'state': {u'code': 1,
                        u'description': u'https://zqedr-api.nais.gov.ua/1.0/subjects/2842336'},
             u'identification': {u'url': u'https://zqedr-api.nais.gov.ua/1.0/subjects/2842336',
                                 u'schema': u'UA-EDR',
                                 u'id': u'123456789',
                                 u'legalName': u'123456789'},
             u'id': 2842336}])

    def test_ipn(self):
        """ Get info by IPN (physical entity-entrepreneur)"""
        setup_routing(self.edr_api_app, func=response_code)
        response = self.app.get('/verify?code=1234567891')
        self.assertEqual(response.status, '200 OK')
        self.assertEqual(response.content_type, 'application/json')
        self.assertEqual(response.json['data'],
                         [{u'state': {u'code': 1,
                                     u'description': u'https://zqedr-api.nais.gov.ua/1.0/subjects/2842335'},
                          u'identification': {u'url': u'https://zqedr-api.nais.gov.ua/1.0/subjects/2842335',
                                              u'schema': u'UA-EDR', u'id': u'1234567891',
                                              u'legalName': u"АКЦІОНЕРНЕ ТОВАРИСТВО КОМЕРЦІЙНИЙ БАНК \"ПРИВАТБАНК\""},
                          u'id': 2842335}])

    def test_invalid_passport(self):
        """Check invalid passport number АБВ"""
        setup_routing(self.edr_api_app, func=response_passport)
        response = self.app.get('/verify?passport=АБВ', status=403)
        self.assertEqual(response.status, '403 Forbidden')
        self.assertEqual(response.content_type, 'application/json')
        self.assertEqual(response.json['errors'][0]['description'],
                         [{u'code': 11, u'message': u'`passport` parameter has wrong value.'}])

    def test_invalid_code(self):
        """Check invalid EDRPOU(IPN) number 123"""
        setup_routing(self.edr_api_app, func=response_code)
        response = self.app.get('/verify?code=123', status=403)
        self.assertEqual(response.content_type, 'application/json')
        self.assertEqual(response.status, '403 Forbidden')
        self.assertEqual(response.json['errors'][0]['description'], [{u'message': u'EDRPOU not found'}])

    def test_unauthorized(self):
        """Send request without token using tests_copy.ini conf file"""
        setup_routing(self.edr_api_app, func=check_headers)
        self.app_copy = webtest.TestApp("config:test_conf/tests_copy.ini", relative_to=os.path.dirname(__file__))
        self.app_copy.authorization = ('Basic', ('robot', 'robot'))
        response = self.app_copy.get('/verify?code=123', status=403)
        self.assertEqual(response.content_type, 'application/json')
        self.assertEqual(response.status, '403 Forbidden')
        self.assertEqual(response.json['errors'][0]['description'],
                         [{u'message': u'Authentication credentials were not provided.', u'code': 1}])

    def test_payment_required(self):
        """Check 402 status EDR response"""
        setup_routing(self.edr_api_app, func=payment_required)
        response = self.app.get('/verify?code=14360570', status=403)
        self.assertEqual(response.content_type, 'application/json')
        self.assertEqual(response.status, '403 Forbidden')
        self.assertEqual(response.json['errors'][0]['description'], [{u'message': u'Paiment required.', u'code': 5}])

    def test_forbidden(self):
        """Check 403 status EDR response"""
        setup_routing(self.edr_api_app, func=forbidden)
        response = self.app.get('/verify?code=14360570', status=403)
        self.assertEqual(response.content_type, 'application/json')
        self.assertEqual(response.status, '403 Forbidden')
        self.assertEqual(response.json['errors'][0]['description'],
                         [{u'message': u'Your account is not permitted to access this resource.', u'code': 3}])

    def test_invalid_token(self):
        """Send request with invalid token 123 using new tests_copy_2.ini conf file"""
        setup_routing(self.edr_api_app, func=check_headers)
        self.app_copy = webtest.TestApp("config:test_conf/tests_copy_2.ini", relative_to=os.path.dirname(__file__))
        self.app_copy.authorization = ('Basic', ('robot', 'robot'))
        response = self.app_copy.get('/verify?code=123', status=403)
        self.assertEqual(response.content_type, 'application/json')
        self.assertEqual(response.status, '403 Forbidden')
        self.assertEqual(response.json['errors'][0]['description'],
                         [{u'code': 2, u'message': u'Invalid or expired token.'}])

    def test_not_acceptable(self):
        """Check 406 status EDR response"""
        setup_routing(self.edr_api_app, func=not_acceptable)
        response = self.app.get('/verify?code=123', status=403)
        self.assertEqual(response.content_type, 'application/json')
        self.assertEqual(response.status, '403 Forbidden')
        self.assertEqual(response.json['errors'][0]['description'], [{u'message': u'Message.'}])

    def test_too_many_requests(self):
        """Check 429 status EDR response(too many requests)"""
        setup_routing(self.edr_api_app, func=too_many_requests)
        response = self.app.get('/verify?code=123', status=403)
        self.assertEqual(response.content_type, 'application/json')
        self.assertEqual(response.status, '403 Forbidden')
        self.assertEqual(response.json['errors'][0]['description'], [{u'message': u'Retry request after 26 seconds.'}])

    def test_server_error(self):
        """Check 500 status EDR response"""
        setup_routing(self.edr_api_app, func=server_error)
        response = self.app.get('/verify?code=123', status=403)
        self.assertEqual(response.content_type, 'application/json')
        self.assertEqual(response.status, '403 Forbidden')
        self.assertEqual(response.json['errors'][0]['description'], [{u'message': u'Internal error.', u'code': 20}])

    def test_bad_gateway(self):
        """Check 502 status EDR response"""
        setup_routing(self.edr_api_app, func=bad_gateway)
        response = self.app.get('/verify?code=123', status=403)
        self.assertEqual(response.content_type, 'application/json')
        self.assertEqual(response.status, '403 Forbidden')
        self.assertEqual(response.json['errors'][0]['description'], [{u'message': u'Service is disabled or upgrade.'}])

    def test_two_error_messages(self):
        """Check when EDR passes two errors in response"""
        setup_routing(self.edr_api_app, func=two_error_messages)
        response = self.app.get('/verify?code=123', status=403)
        self.assertEqual(response.content_type, 'application/json')
        self.assertEqual(response.status, '403 Forbidden')
        self.assertEqual(response.json['errors'][0]['description'], [{u'code': 0, u'message': u'Message1.'},
                                                                     {u'code': 0, u'message': u'Message2.'}])

    def test_long_edrpou(self):
        setup_routing(self.edr_api_app, func=response_passport)
        response = self.app.get('/verify?passport=12345678912', status=403)
        self.assertEqual(response.status, '403 Forbidden')
        self.assertEqual(response.content_type, 'application/json')
        self.assertEqual(response.json['errors'][0]['description'],
                         [{u'message': u'`passport` parameter has wrong value.', u'code': 11}])

    def test_empty_request(self):
        """ Send request without params  """
        setup_routing(self.edr_api_app, func=response_passport)
        response = self.app.get('/verify', status=403)
        self.assertEqual(response.status, '403 Forbidden')
        self.assertEqual(response.content_type, 'application/json')
        self.assertEqual(response.json['errors'][0]['description'],
                         [{u'message': u'Need pass code or passport'}])

    def test_accept_yaml(self):
        setup_routing(self.edr_api_app, func=response_code)
        response = self.app.get('/verify?code=14360570', headers={'Accept': 'application/yaml'})
        self.assertEqual(response.status, '200 OK')
        self.assertEqual(response.content_type, 'application/yaml')
        with open(os.path.join(os.path.dirname(__file__), 'test_data.yaml'), 'r') as f:
            test_yaml_data = f.read()
        self.assertEqual(response.body, test_yaml_data)


class TestDetails(BaseWebTest):
    """ Test details view """

    def test_details(self):
        """Check data for get_subject_details request"""
        setup_routing(self.edr_api_app, path='/1.0/subjects/2842335', func=response_details)
        response = self.app.get('/details/2842335')
        self.assertEqual(response.status, '200 OK')
        self.assertEqual(response.content_type, 'application/json')
        self.assertEqual(response.json['data'], {
            u"additionalActivityKinds": [
                {u"scheme": u"КВЕД",
                 u"id": u"64.92",
                 u"description": u"Інші види кредитування"},
                {u"scheme": u"КВЕД",
                 u"id": u"64.99",
                 u"description": u"Надання інших фінансових послуг (крім страхування та пенсійного забезпечення), н. в. і. у."},
                {u"scheme": u"КВЕД",
                 u"id": u"66.11",
                 u"description": u"Управління фінансовими ринками"},
                {u"scheme": u"КВЕД",
                 u"id": u"66.12",
                 u"description": u"Посередництво за договорами по цінних паперах або товарах"},
                {u"scheme": u"КВЕД",
                 u"id": u"66.19",
                 u"description": u"Інша допоміжна діяльність у сфері фінансових послуг, крім страхування та пенсійного забезпечення"}],
            u"management": u"ЗАГАЛЬНІ ЗБОРИ",
            u"name": u"ПАТ КБ \"ПРИВАТБАНК\"",
            u"identification": {u"scheme": u"UA-EDR",
                               u"id": u"14360570",
                               u"legalName": u"АКЦІОНЕРНЕ ТОВАРИСТВО КОМЕРЦІЙНИЙ БАНК \"ПРИВАТБАНК\""},
            u"address": {u"postalCode": u"49094",
                        u"countryName": u"УКРАЇНА",
                        u"streetAddress": u"Дніпропетровська обл., місто Дніпропетровськ, Жовтневий район ВУЛИЦЯ НАБЕРЕЖНА ПЕРЕМОГИ буд. 50"},
            u"founders": [{u"role_text": u"засновник",
                          u"role": 4,
                          u"name": u"АКЦІОНЕРИ - ЮРИДИЧНІ ТА ФІЗИЧНІ ОСОБИ",
                          u"address": None,
                          u"capital": 18100740000}],
            u"activityKind": {u"scheme": u"КВЕД",
                             u"id": u"64.19",
                             u"description": u"Інші види грошового посередництва"}
        })

    def test_too_many_requests_details(self):
        """Check 429 status EDR response(too many requests) for details request"""
        setup_routing(self.edr_api_app, path='/1.0/subjects/2842335', func=too_many_requests_details)
        response = self.app.get('/details/2842335', status=403)
        self.assertEqual(response.content_type, 'application/json')
        self.assertEqual(response.status, '403 Forbidden')
        self.assertEqual(response.json['errors'][0]['description'], [{u'message': u'Retry request after 26 seconds.'}])

    def test_bad_gateway_details(self):
        """Check 502 status EDR response"""
        setup_routing(self.edr_api_app, path='/1.0/subjects/2842335', func=bad_gateway_details)
        response = self.app.get('/details/2842335', status=403)
        self.assertEqual(response.content_type, 'application/json')
        self.assertEqual(response.status, '403 Forbidden')
        self.assertEqual(response.json['errors'][0]['description'], [{u'message': u'Service is disabled or upgrade.'}])

    def test_accept_yaml_details(self):
        setup_routing(self.edr_api_app, path='/1.0/subjects/2842335',func=response_details)
        response = self.app.get('/details/2842335', headers={'Accept': 'application/yaml'})
        self.assertEqual(response.status, '200 OK')
        self.assertEqual(response.content_type, 'application/yaml')
        with open(os.path.join(os.path.dirname(__file__), 'test_data_details.yaml'), 'r') as f:
            test_yaml_data = f.read()
        self.assertEqual(response.body, test_yaml_data)


class TestVerifyPlatform(TestVerify):

    def setUp(self):
        self.app.authorization = ('Basic', ('platform', 'platform'))

