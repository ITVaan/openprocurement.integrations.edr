# -*- coding: utf-8 -*-
import requests


class EdrClient(object):
    """Base class for making requests to EDR"""

    def __init__(self, host, token, timeout=None, port=443):
        self.token = token
        self.url = '{host}:{port}/1.0/subjects'.format(host=host, port=port)
        self.headers = {"Accept": "application/json",
                        "Authorization": "Token {token}".format(token=self.token)}
        self.timeout = timeout

    def get_subject(self, code):
        """
        Send request to EDR using EDRPOU (physical entity-entrepreneur) code or passport.
        In response we except list of subjects with unique id in each subject.
        List mostly contains 1 subject, but occasionally includes 2 or none.
        """
        param = 'code' if code.isdigit() and (len(code) == 8 or len(code) == 10) else 'passport'  # find out we accept edrpou or passport code
        url = '{url}?{param}={code}'.format(url=self.url, param=param, code=code)
        response = requests.get(url=url, headers=self.headers, timeout=self.timeout)

        return response

    def get_subject_details(self, edr_unique_id):
        """
        Send request to ERD using unique identifier to get subject's details.
        """
        url = '{url}/{id}'.format(url=self.url, id=edr_unique_id)
        response = requests.get(url=url, headers=self.headers, timeout=self.timeout)

        return response

