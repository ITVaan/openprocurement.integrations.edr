# -*- coding: utf-8 -*-
from gevent import monkey
monkey.patch_all()

try:
    import urllib3.contrib.pyopenssl
    urllib3.contrib.pyopenssl.inject_into_urllib3()
except ImportError:
    pass

import logging.config
import gevent

from openprocurement.integrations.edr.databridge.journal_msg_ids import (
    DATABRIDGE_GET_TENDER_FROM_QUEUE, DATABRIDGE_TENDER_PROCESS, DATABRIDGE_START)
from openprocurement.integrations.edr.databridge.utils import generate_req_id, journal_context
from openprocurement.integrations.edr.databridge.utils import Data

logger = logging.getLogger("openprocurement.integrations.edr.databridge")


class FilterTenders(object):
    """ Edr API Data Bridge """

    def __init__(self, tenders_sync_client, filtered_tenders_queue, data_queue, delay=15):
        super(FilterTenders, self).__init__()

        self.delay = delay
        # init clients
        self.tenders_sync_client = tenders_sync_client

        # init queues for workers
        self.filtered_tenders_queue = filtered_tenders_queue
        self.data_queue = data_queue

    def prepare_data(self):
        while True:
            tender_id = self.filtered_tenders_queue.get()
            try:
                tender = self.tenders_sync_client.get_tender(tender_id,
                                                             extra_headers={'X-Client-Request-ID': generate_req_id()})['data']
                logger.info('Get tender {} from filtered_tenders_queue'.format(tender_id),
                            extra=journal_context({"MESSAGE_ID": DATABRIDGE_GET_TENDER_FROM_QUEUE},
                            params={"TENDER_ID": tender['id']}))
            except Exception as e:
                logger.warning('Fail to get tender info {}'.format(tender_id),
                               extra=journal_context(params={"TENDER_ID": tender['id']}))
                logger.exception(e)
                logger.info('Put tender {} back to tenders queue'.format(tender_id),
                            extra=journal_context(params={"TENDER_ID": tender['id']}))
                self.filtered_tenders_queue.put(tender_id)
            else:
                if 'awards' in tender:
                    for award in tender['awards']:
                        logger.info('Processing tender {} award {}'.format(tender['id'], award['id']),
                                    extra=journal_context({"MESSAGE_ID": DATABRIDGE_TENDER_PROCESS},
                                    params={"TENDER_ID": tender['id']}))
                        if award['status'] == 'pending' and not [document for document in award.get('documents', [])
                                                                 if document.get('documentType') == 'registerExtract']:
                            for supplier in award['suppliers']:
                                tender_data = Data(tender['id'], award['id'], supplier['identifier']['id'], 'awards', None)
                                self.data_queue.put(tender_data)
                        else:
                            logger.info('Tender {} award {} is not in status pending or award has already document '
                                        'with documentType registerExtract.'.format(tender_id, award['id']),
                                        extra=journal_context(params={"TENDER_ID": tender['id']}))
                elif 'bids' in tender:
                    for qualification in tender['qualifications']:
                        if qualification['status'] == 'pending' and \
                                not [document for document in qualification.get('documents', [])
                                     if document.get('documentType') == 'registerExtract']:
                            appropriate_bid = [b for b in tender['bids'] if b['id'] == qualification['bidID']][0]
                            tender_data = Data(tender['id'], qualification['id'],
                                               appropriate_bid['tenderers'][0]['identifier']['id'], 'qualifications', None)
                            self.data_queue.put(tender_data)
                            logger.info('Processing tender {} bid {}'.format(tender['id'], appropriate_bid['id']),
                                        extra=journal_context({"MESSAGE_ID": DATABRIDGE_TENDER_PROCESS},
                                                              params={"TENDER_ID": tender['id']}))
                        else:
                            logger.info('Tender {} qualification {} is not in status pending or qualification has '
                                        'already document with documentType registerExtract.'.format(
                                            tender_id, qualification['id']),
                                            extra=journal_context(params={"TENDER_ID": tender['id']}))

    def run(self):
        logger.info('Start Filter Tenders', extra=journal_context({"MESSAGE_ID": DATABRIDGE_START}, {}))
        self.job = gevent.spawn(self.prepare_data)

        try:
            while True:
                gevent.sleep(self.delay)
                if self.job.dead:
                    logger.warning("filter Tenders job die try restart.")
                    self.job = gevent.spawn(self.prepare_data)
                    logger.info("filter tenders job restarted.")
        except Exception as e:
            logger.error(e)
            self.job.kill(timeout=5)
