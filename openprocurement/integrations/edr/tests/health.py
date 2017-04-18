from openprocurement.integrations.edr.tests.base import BaseWebTest
from openprocurement.integrations.edr.utils import ROUTE_PREFIX


class TestHealth(BaseWebTest):
    """ Test health view"""

    def test_get(self):
        response = self.app.get('/health')
        self.assertEqual(response.status, "200 OK")
        self.assertEqual(response.content_type, "application/json")
