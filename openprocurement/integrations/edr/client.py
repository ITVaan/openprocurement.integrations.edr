# -*- coding: utf-8 -*-
import requests
import base64


class EdrClient(object):
    """Base class for making requests to EDR"""

    def __init__(self, host, token, timeout=None, port=443):
        self.session = requests.Session()
        self.token = token
        self.url = '{host}:{port}/1.0/subjects'.format(host=host, port=port)
        self.headers = {"Accept": "application/json",
                        "Authorization": "Token {token}".format(token=self.token)}
        self.timeout = timeout

    def get_subject(self, param, code):
        """
        Send request to EDR using EDRPOU (physical entity-entrepreneur) code or passport.
        In response we except list of subjects with unique id in each subject.
        List mostly contains 1 subject, but occasionally includes 2 or none.
        """
        url = '{url}?{param}={code}'.format(url=self.url, param=param, code=code)
        response = self.session.get(url=url, headers=self.headers, timeout=self.timeout)

        return response

    def get_subject_details(self, edr_unique_id):
        """
        Send request to ERD using unique identifier to get subject's details.
        """
        url = '{url}/{id}'.format(url=self.url, id=edr_unique_id)
        response = self.session.get(url=url, headers=self.headers, timeout=self.timeout)

        return response


class DocServiceClient(object):
    """Base class for making requests to Document Service"""

    def __init__(self, host, user, password, port=6555, timeout=None):
        self.session = requests.Session()
        self.url = '{host}:{port}/upload'.format(host=host, port=port)
        self.user = user
        self.password = password
        self.timeout = timeout

    def upload(self, filename, in_file, content_type):
        files = {'file': (filename, in_file, content_type)}
        response = self.session.post(url=self.url, auth=(self.user, self.password), timeout=self.timeout, files=files)

        return response


class ProxyClient(object):
    """Base class for making requests to Proxy server"""

    def __init__(self, host, token, timeout=None, port=6547):
        self.session = requests.Session()
        self.token = token
        self.verify_url = '{host}:{port}/verify'.format(host=host, port=port)
        self.details_url = '{host}:{port}/details'.format(host=host, port=port)
        self.headers = {"Authorization": "Basic {token}".format(token=self.token)}
        self.timeout = timeout

    def verify(self, param, code):
        """Send request to Proxy server to verify EDRPOU code"""
        url = '{url}?{param}={code}'.format(url=self.verify_url, param=param, code=code)
        response = self.session.get(url=url, headers=self.headers, timeout=self.timeout)

        return response

    def details(self, id):
        """ Send request to Proxy server to get details."""
        url = '{url}/{id}'.format(url=self.details_url, id=id)
        response = self.session.get(url=url, headers=self.headers, timeout=self.timeout)

        return response
