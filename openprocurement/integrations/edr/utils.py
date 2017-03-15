# -*- coding: utf-8 -*-
import os
from functools import partial
from logging import getLogger
from json import dumps
from cornice.util import json_error
from pkg_resources import get_distribution
from cornice.resource import resource, view
from webob.multidict import NestedMultiDict
from datetime import datetime
from pytz import timezone
from hashlib import sha512
from ConfigParser import ConfigParser
from pyramid.security import Allow


PKG = get_distribution(__package__)
LOGGER = getLogger(PKG.project_name)
VERSION = '{}.{}'.format(int(PKG.parsed_version[0]), int(PKG.parsed_version[1]) if PKG.parsed_version[1].isdigit() else 0)
json_view = partial(view, renderer='json')
TZ = timezone(os.environ['TZ'] if 'TZ' in os.environ else 'Europe/Kiev')
USERS = {}
identification_schema = u'UA-EDR'
activityKind_scheme = u'КВЕД'


class Root(object):
    __name__ = None
    __parent__ = None
    __acl__ = [
        (Allow, 'g:platforms', 'verify'),
        (Allow, 'g:robots', 'verify'),
        (Allow, 'g:robots', 'get_details'),
    ]

    def __init__(self, request):
        self.request = request


def read_users(filename):
    config = ConfigParser()
    config.read(filename)
    for i in config.sections():
        USERS.update(dict([
            (
                j,
                {
                    'password': k,
                    'group': i
                }
            )
            for j, k in config.items(i)
        ]))


def get_now():
    return datetime.now(TZ)


def update_logging_context(request, params):
    if not request.__dict__.get('logging_context'):
        request.logging_context = {}

    for x, j in params.items():
        request.logging_context[x.upper()] = j


def context_unpack(request, msg, params=None):
    if params:
        update_logging_context(request, params)
    logging_context = request.logging_context
    journal_context = msg
    for key, value in logging_context.items():
        journal_context["JOURNAL_" + key] = value
    return journal_context


def error_handler(errors, request_params=True):
    params = {'ERROR_STATUS': errors.status}
    if request_params:
        params['ROLE'] = str(errors.request.authenticated_role)
        if errors.request.params:
            params['PARAMS'] = str(dict(errors.request.params))
    LOGGER.info('Error on processing request "{}"'.format(dumps(errors, indent=4)),
                extra=context_unpack(errors.request, {'MESSAGE_ID': 'error_handler'}, params))
    return json_error(errors)


def forbidden(request):
    request.errors.add('url', 'permission', 'Forbidden')
    request.errors.status = 403
    return error_handler(request.errors)


def add_logging_context(event):
    request = event.request
    params = {
        'API_VERSION': VERSION,
        'TAGS': 'python,api',
        'USER': str(request.authenticated_userid or ''),
        #'ROLE': str(request.authenticated_role),
        'CURRENT_URL': request.url,
        'CURRENT_PATH': request.path_info,
        'REMOTE_ADDR': request.remote_addr or '',
        'USER_AGENT': request.user_agent or '',
        'REQUEST_METHOD': request.method,
        'TIMESTAMP': get_now().isoformat(),
        'REQUEST_ID': request.environ.get('REQUEST_ID', ''),
        'CLIENT_REQUEST_ID': request.headers.get('X-Client-Request-ID', ''),
    }

    request.logging_context = params


def request_params(request):
    try:
        params = NestedMultiDict(request.GET, request.POST)
    except UnicodeDecodeError:
        request.errors.add('body', 'data', 'could not decode params')
        request.errors.status = 422
        raise error_handler(request.errors, False)
    except Exception as e:
        request.errors.add('body', str(e.__class__.__name__), str(e))
        request.errors.status = 422
        raise error_handler(request.errors, False)
    return params


def set_logging_context(event):
    request = event.request
    params = dict()
    params['ROLE'] = str(request.authenticated_role)
    if request.params:
        params['PARAMS'] = str(dict(request.params))
    update_logging_context(request, params)


def set_renderer(event):
    request = event.request
    try:
        json = request.json_body
    except ValueError:
        json = {}
    pretty = isinstance(json, dict) and json.get('options', {}).get('pretty') or request.params.get('opt_pretty')
    accept = request.headers.get('Accept')
    jsonp = request.params.get('opt_jsonp')
    if jsonp and pretty:
        request.override_renderer = 'prettyjsonp'
        return True
    if jsonp:
        request.override_renderer = 'jsonp'
        return True
    if pretty:
        request.override_renderer = 'prettyjson'
        return True
    if accept == 'application/yaml':
        request.override_renderer = 'yaml'
        return True


def auth_check(username, password, request):
    if username in USERS and USERS[username]['password'] == sha512(password).hexdigest():
        return ['g:{}'.format(USERS[username]['group'])]


def prepare_data(data):
    return {'id': data.get('id'),
            'state': {'code': data.get('state'),
                      'description': data.get('state_text')},
            'identification': {'schema': identification_schema,
                               'id': data.get('code'),
                               'legalName': data.get('name'),
                               'url': data.get('url')}}


def prepare_data_details(data):
    additional_activity_kinds = []
    primary_activity_kind = {}
    for activity_kind in data.get('activity_kinds', []):
        if activity_kind.get('is_primary'):
            primary_activity_kind = {'id': activity_kind.get('code'),
                                     'scheme': activityKind_scheme,
                                     'description': activity_kind.get('name')}
        else:
            additional_activity_kinds.append({'id': activity_kind.get('code'),
                                              'scheme': activityKind_scheme,
                                              'description': activity_kind.get('name')})
    return {'name': data.get('names').get('short') if data.get('names') else None,
            'identification': {'scheme': identification_schema,
                               'id': data.get('code'),
                               'legalName': data.get('names').get('display') if data.get('names') else None},
            'founders': data.get('founders'),
            'management': data.get('management'),
            'activityKind': primary_activity_kind or None,
            'additionalActivityKinds': additional_activity_kinds or None,
            'address': {'streetAddress': data.get('address').get('address') if data.get('address') else None,
                        'postalCode': data.get('address').get('zip') if data.get('address') else None,
                        'countryName': data.get('address').get('country') if data.get('address') else None}}
