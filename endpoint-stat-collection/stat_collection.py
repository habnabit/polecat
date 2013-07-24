# Copyright (c) Aaron Gallagher <_@habnab.it>
# See COPYING for details.

from twisted.internet import protocol, defer
from twisted.protocols import amp

import time


class FetchEndpointRequestLengthStats(amp.Command):
    arguments = []
    response = [
        ('endpoints', amp.AmpList([
            ('endpoint', amp.String()),
            ('lengths', amp.ListOf(amp.Float())),
        ])),
    ]


class ReconnectingAMP(amp.AMP):
    def connectionMade(self):
        self.factory.resetDelay()
        self.factory.clients.add(self)

    def connectionLost(self, reason):
        self.factory.clients.discard(self)
        amp.AMP.connectionLost(self, reason)


class ReconnectingAMPFactory(protocol.ReconnectingClientFactory):
    protocol = ReconnectingAMP

    def __init__(self, clients):
        self.clients = clients


class ClientPoller(object):
    def __init__(self, database):
        self.database = database
        self.clients = set()

    @defer.inlineCallbacks
    def poll(self):
        now = time.time()
        results = yield defer.DeferredList([
            client.callRemote(FetchEndpointRequestLengthStats)
            for client in self.clients])
        cursor = self.database.cursor()
        cursor.executemany(
            'INSERT INTO endpoint_stats (recorded_at, endpoint, length) VALUES (?, ?, ?)',
            [(now, endpoint_results['endpoint'], length)
             for success, result in results if success
             for endpoint_results in result['endpoints']
             for length in endpoint_results['lengths']])
        self.database.commit()
