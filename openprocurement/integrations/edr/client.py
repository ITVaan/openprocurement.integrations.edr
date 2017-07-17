# -*- coding: utf-8 -*-
import requests

from openprocurement.integrations.edr.timeout_handler import TimeoutHandler

from logging import getLogger
logger = getLogger(__name__)

class EdrClient(object):
    """Base class for making requests to EDR"""

    def __init__(self, host, token, port=443, timeout_min=1, timeout_max=300, timeout_step=2, timeout_mode='mult'):
        self.session = requests.Session()
        self.token = token
        self.url = '{host}:{port}/1.0/subjects'.format(host=host, port=port)
        self.headers = {"Accept": "application/json",
                        "Authorization": "Token {token}".format(token=self.token)}

        self.timeout_verify = TimeoutHandler(timeout_min, timeout_max, timeout_step, timeout_mode)
        self.timeout_details = TimeoutHandler(timeout_min, timeout_max, timeout_step, timeout_mode)

    def _do_request(self, url, timeout):
        try:
            response = self.session.get(url=url, headers=self.headers, timeout=timeout.value)
            timeout.update(True)
            return response
        except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectTimeout, requests.exceptions.Timeout):
            if not timeout.update(False):
                logger.fatal('Timeout maxed out! Value: {0}'.format(timeout.value))

            raise

    def get_subject(self, param, code):
        """
        Send request to EDR using EDRPOU (physical entity-entrepreneur) code or passport.
        In response we except list of subjects with unique id in each subject.
        List mostly contains 1 subject, but occasionally includes 2 or none.
        """
        return self._do_request('{url}?{param}={code}'.format(url=self.url, param=param, code=code), self.timeout_verify)

    def get_subject_details(self, edr_unique_id):
        """
        Send request to EDR using unique identifier to get subject's details.
        """
        return self._do_request('{url}/{id}'.format(url=self.url, id=edr_unique_id), self.timeout_details)
