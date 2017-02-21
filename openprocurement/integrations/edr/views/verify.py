# -*- coding: utf-8 -*-
import requests
from collections import namedtuple
from openprocurement.api.utils import (
    json_view,
)
from openprocurement.integrations.edr.utils import opresource, APIResource

EDRDetails = namedtuple("EDRDetails", ['param', 'code'])


@opresource(name='Verify customer',
            path='/verify',
            description="Verify customer by edr code ")
class VerifyResource(APIResource):
    """ Verify customer """

    def handle_error(self, message):
        self.request.errors.add('body', 'data', message)
        self.request.errors.status = 403

    @json_view(permission='verify')
    def get(self):
        code = self.request.params.get('code', '').encode('utf-8')
        details = EDRDetails('code', code)
        if not code:
            passport = self.request.params.get('passport', '').encode('utf-8')
            if not passport:
                self.request.errors.add('body', 'data', [{u'message': u'Need pass code or passport as GET params.'}])
                self.request.errors.status = 403
                return
            details = EDRDetails('passport', passport)
        try:
            response = self.edr_api.get_subject(**details._asdict())
        except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectTimeout):
            self.handle_error([{u'message': u'Gateway Timeout Error'}])
            return
        if response.status_code == 200:
            data = response.json()
            if not data:
                self.LOGGER.warning('Accept empty response from EDR service for {}'.format(details.code))
                self.handle_error([{u'message': u'EDRPOU not found'}])
                return
            self.LOGGER.info('Return data from EDR service for {}'.format(details.code))
            return {'data': data}
        elif response.status_code == 429:
            self.handle_error([{u'message': u'Retry request after {} seconds.'.format(response.headers.get('Retry-After'))}])
            return
        elif response.status_code == 502:
            self.handle_error([{u'message': u'Service is disabled or upgrade.'}])
            return
        else:
            self.handle_error(response.json()['errors'])
            return
