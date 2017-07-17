# -*- coding: utf-8 -*-
import json

import requests
from collections import namedtuple
from pyramid.view import view_config
from logging import getLogger
from openprocurement.integrations.edr.utils import (prepare_data_details, prepare_data, error_handler, meta_data,
                                                    get_sandbox_data, db_key)

LOGGER = getLogger(__name__)
EDRDetails = namedtuple("EDRDetails", ['param', 'code'])
default_error_status = 403
error_message_404 = {u"errorDetails": u"Couldn't find this code in EDR.", u"code": u"notFound"}
lifetime_const = 300

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
            return error_handler(request, default_error_status, {"location": "url", "name": "id",
                                                                 "description": [{u'message': u'Wrong name of the GET parameter'}]})
        details = EDRDetails('passport', passport)
    if request.registry.cache_db.has(db_key(details.code, role)):
        LOGGER.info("Code {} was found in cache".format(details.code))
        return json.loads(request.registry.cache_db.get(db_key(details.code, role)))
    LOGGER.debug("Code {} was not found in cache".format(details.code,))
    data = get_sandbox_data(request, role, code)  # return test data if SANDBOX_MODE=True and data exists for given code
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
            res = error_handler(request, 404, {"location": "body", "name": "data",
                                               "description": [{u"error": error_message_404,
                                                                u'meta': {"sourceDate": meta_data(
                                                                     response.headers['Date'])}}]})
            request.registry.cache_db.put(db_key(details.code, role), json.dumps(res), ex=request.registry.time_to_live)
            return res
        if role == 'robots':  # get details for edr-bot
            data_details = user_details(request, [obj['id'] for obj in data])
            return data_details
        return {'data': [prepare_data(d) for d in data], 'meta': {'sourceDate': meta_data(response.headers['Date'])}}
    else:
        return handle_error(request, response)


def user_details(request, internal_ids):
    """Composes array of detailed reference files"""
    data = []
    details_source_date = []
    for internal_id in internal_ids:
        if request.registry.cache_db.has("i_"+str(internal_id)):
            redis_data = json.loads(request.registry.cache_db.get(internal_id))
            data.append(redis_data['data'])
            details_source_date.append(redis_data['meta'])
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
            caching_data = {"data": prepare_data_details(response.json()), "meta": meta_data(response.headers['Date'])}
            data.append(prepare_data_details(response.json()))
            details_source_date.append(meta_data(response.headers['Date']))
            request.registry.cache_db.put(db_key(internal_id, request.authenticated_role),
                                          json.dumps(caching_data), ex=request.registry.time_to_live)
    return {"data": data, "meta": {"sourceDate": details_source_date[-1], "detailsSourceDate": details_source_date}}
