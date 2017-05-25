# -*- coding: utf-8 -*-
import json
import os
from datetime import datetime

import openprocurement.integrations.edr.tests.base as base_test

from openprocurement.integrations.edr.tests.base import BaseWebTest
from openprocurement.integrations.edr.tests._server import (setup_routing, response_code, response_details,
                                                            payment_required, too_many_requests, response_passport,
                                                            check_headers)
from webtest import TestApp

now = datetime.now()

edrpou = u"14360570"
ipn = u"1234567891"
passport = u"АБ123456"
invalid_passport = u"АБВ"
invalid_edrpou = u"123"
x_edrInternalId = u"2842335"


class DumpsTestAppwebtest(TestApp):
    def do_request(self, req, status=None, expect_errors=None):
        req.headers.environ["HTTP_HOST"] = "api-sandbox.openprocurement.org"
        origin_path = req.headers.environ["PATH_INFO"]  # save original path
        req.headers.environ["PATH_INFO"] = '/api/1.0{original_path}'.format(original_path=origin_path)  # add version

        if hasattr(self, 'file_obj') and not self.file_obj.closed:
            self.file_obj.write(req.as_bytes(True))
            self.file_obj.write("\n")
            if req.body:
                try:
                    self.file_obj.write(
                            'DATA:\n' + json.dumps(json.loads(req.body), indent=2, ensure_ascii=False).encode('utf8'))
                except ValueError:
                    pass  # doesn't write anything
                self.file_obj.write("\n")
            self.file_obj.write("\n")

        req.headers.environ["PATH_INFO"] = origin_path  # set original path
        resp = super(DumpsTestAppwebtest, self).do_request(req, status=status, expect_errors=expect_errors)
        if hasattr(self, 'file_obj') and not self.file_obj.closed:
            headers = [(n.title(), v)
                       for n, v in resp.headerlist
                       if n.lower() != 'content-length']
            headers.sort()
            self.file_obj.write(str('Response: %s\n%s\n') % (
                resp.status,
                str('\n').join([str('%s: %s') % (n, v) for n, v in headers]),
            ))

            if resp.testbody:
                try:
                    self.file_obj.write(json.dumps(json.loads(resp.testbody), indent=2, ensure_ascii=False).encode('utf8'))
                except ValueError:
                    pass
            self.file_obj.write("\n\n")
        return resp


class TenderResourceTest(BaseWebTest):

    def setUp(self):
        self.app = DumpsTestAppwebtest("config:tests.ini", relative_to=os.path.dirname(base_test.__file__))
        self.app.authorization = ('Basic', ('platform', 'platform'))

    def test_docs_tutorial(self):
        request_path = '/verify?{}={}'
        details_path = '/details/{}'
        setup_routing(self.edr_api_app, func=response_code)

        # Basic request

        with open('docs/source/tutorial/basic_request.http', 'w') as self.app.file_obj:
            self.app.authorization = ('Basic', ('platform', 'platform'))
            response = self.app.get(request_path.format('id', edrpou))
            self.assertEqual(response.status, '200 OK')
            self.app.file_obj.write("\n")

        setup_routing(self.edr_api_app, func=response_code)

        # request with individual tax number

        with open('docs/source/tutorial/ipn.http', 'w') as self.app.file_obj:
            self.app.authorization = ('Basic', ('platform', 'platform'))
            response = self.app.get(request_path.format('id', ipn))
            self.assertEqual(response.status, '200 OK')
            self.app.file_obj.write("\n")

        setup_routing(self.edr_api_app, func=payment_required)

        # Payment required

        with open('docs/source/tutorial/payment_requests.http', 'w') as self.app.file_obj:
            self.app.authorization = ('Basic', ('platform', 'platform'))
            response = self.app.get(request_path.format('id', edrpou), status=403)
            self.assertEqual(response.status, '403 Forbidden')
            self.app.file_obj.write("\n")

        setup_routing(self.edr_api_app, func=response_code)

        # empty response
        with open('docs/source/tutorial/empty_response.http', 'w') as self.app.file_obj:
            self.app.authorization = ('Basic', ('platform', 'platform'))
            response = self.app.get(request_path.format('id', invalid_edrpou), status=404)
            self.assertEqual(response.status, '404 Not Found')
            self.app.file_obj.write("\n")

        setup_routing(self.edr_api_app, func=too_many_requests)

        # Too many requests

        with open('docs/source/tutorial/too_many_requests.http', 'w') as self.app.file_obj:
            self.app.authorization = ('Basic', ('platform', 'platform'))
            response = self.app.get(request_path.format('id', edrpou), status=429)
            self.assertEqual(response.status, '429 Too Many Requests')
            self.app.file_obj.write("\n")

        setup_routing(self.edr_api_app, func=response_passport)

        # Pass pass id or passport

        with open('docs/source/tutorial/without_param.http', 'w') as self.app.file_obj:
            self.app.authorization = ('Basic', ('platform', 'platform'))
            response = self.app.get('/verify', status=403)
            self.assertEqual(response.status, '403 Forbidden')
            self.app.file_obj.write("\n")

        setup_routing(self.edr_api_app, path='/1.0/subjects/{}'.format(x_edrInternalId), func=response_details)

        # details
        with open('docs/source/tutorial/details.http', 'w') as self.app.file_obj:
            self.app.authorization = ('Basic', ('robot', 'robot'))
            response = self.app.get(details_path.format(x_edrInternalId))
            self.assertEqual(response.status, '200 OK')
            self.app.file_obj.write("\n")

    def test_auth_errors(self):
        request_path = '/verify?{}={}'

        setup_routing(self.edr_api_app, func=check_headers)
        # Invalid token
        self.app = DumpsTestAppwebtest("config:test_conf/tests_copy_2.ini",
                                       relative_to=os.path.dirname(base_test.__file__))
        with open('docs/source/tutorial/invalid_token.http', 'w') as self.app.file_obj:
            self.app.authorization = ('Basic', ('platform', 'platform'))
            response = self.app.get(request_path.format('id', edrpou), status=403)
            self.assertEqual(response.status, '403 Forbidden')
            self.app.file_obj.write("\n")

