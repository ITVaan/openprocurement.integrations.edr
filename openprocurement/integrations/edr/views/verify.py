# -*- coding: utf-8 -*-
import requests
from collections import namedtuple
from pyramid.view import view_config
from logging import getLogger
from openprocurement.integrations.edr.utils import (prepare_data_details, prepare_data, error_handler, meta_data,
    get_sandbox_data)

LOGGER = getLogger(__name__)
EDRDetails = namedtuple("EDRDetails", ['param', 'code'])
default_error_status = 403
error_message_404 = {u"errorDetails": u"Couldn't find this code in EDR.", u"code": u"notFound"}


def handle_error(request, response):
    if response.headers['Content-Type'] != 'application/json':
        return error_handler(request, default_error_status, {"location": "request", "name": "ip",
                                                             "description": [{u'message': u'Forbidden'}]})
    if response.status_code == 429:
        seconds_to_wait = response.headers.get('Retry-After')
        request.response.headers['Retry-After'] = seconds_to_wait
        return error_handler(request, 429, {"location": "body", "name": "data",
                                            "description": [{u'message': u'Retry request after {} seconds.'.format(seconds_to_wait)}]})
    elif response.status_code == 502:
        return error_handler(request, default_error_status, {"location": "body", "name": "data",
                                                             "description": [{u'message': u'Service is disabled or upgrade.'}]})
    return error_handler(request, default_error_status, {"location": "body", "name": "data",
                                                         "description": response.json()['errors']})


@view_config(route_name='verify', renderer='json',
             request_method='GET', permission='verify')
def verify_user(request):
    code = request.params.get('id', '').encode('utf-8')
    details = EDRDetails('code', code)
    role = request.authenticated_role
    if not code:
        passport = request.params.get('passport', '').encode('utf-8')
        if not passport:
            return error_handler(request, default_error_status, {"location": "url", "name": "id", "description":
                                                                 [{u'message': u'Need pass id or passport'}]})
        details = EDRDetails('passport', passport)

    data = get_sandbox_data(role, code)  # return test data if SANDBOX_MODE=True and data exists for given code
    if data:
        return data

    try:
        response = request.registry.edr_client.get_subject(**details._asdict())
    except (requests.exceptions.ReadTimeout,
            requests.exceptions.ConnectTimeout):
        return error_handler(request, default_error_status, {"location": "url", "name": "id",
                                                             "description": [{u'message': u'Gateway Timeout Error'}]})
    if response.status_code == 200:
        data = response.json()
        if not data:
            LOGGER.warning('Accept empty response from EDR service for {}'.format(details.code))
            return error_handler(request, 404, {"location": "body", "name": "data",
                                                "description": [{u"error": error_message_404,
                                                                 u'meta': meta_data(response.headers['Date'])}]})
        if role == 'robots':  # get details for edr-bot
            data_details = user_details(request, [obj['id'] for obj in data])
            return data_details
        return {'data': [prepare_data(d) for d in data], 'meta': meta_data(response.headers['Date'])}
    else:
        return handle_error(request, response)


def user_details(request, internal_ids):
    data = []
    for internal_id in internal_ids:
        try:
            response = request.registry.edr_client.get_subject_details(internal_id)
        except (requests.exceptions.ReadTimeout,
                requests.exceptions.ConnectTimeout):
            return error_handler(request, default_error_status, {"location": "url", "name": "id",
                                                                 "description": [{u'message': u'Gateway Timeout Error'}]})
        if response.status_code != 200:
            return handle_error(request, response)
        else:
            LOGGER.info('Return detailed data from EDR service for {}'.format(internal_id))
            data.append({'data': prepare_data_details(response.json()),
                         'meta': meta_data(response.headers['Date'])})
    return data
