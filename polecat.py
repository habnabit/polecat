from __future__ import division

from twisted.internet.protocol import Factory
from twisted.internet import defer
from twisted.protocols import amp
from twisted.web.http import HTTPChannel, INTERNAL_SERVER_ERROR
from twisted.web.resource import Resource
from twisted.web.server import Site

from bisect import bisect_left
import collections
import time


def percentile(sorted_data, percentile):
    index = int((len(sorted_data) - 1) * (percentile / 100))
    return sorted_data[index]


class PolecatHTTPChannel(HTTPChannel):
    def connectionMade(self):
        HTTPChannel.connectionMade(self)
        self.site.gotClient()

    def connectionLost(self, reason):
        HTTPChannel.connectionLost(self, reason)
        self.site.lostClient()


class PolecatSite(Site):
    protocol = PolecatHTTPChannel

    requestCount = 0
    errorCount = 0
    activeClients = 0
    _clientsStoppedDeferred = None

    def __init__(self, *a, **kw):
        self.endpointFunc = kw.pop('endpointFunc', None)
        Site.__init__(self, *a, **kw)
        self.endpointRequestLengths = collections.defaultdict(list)
        self.requestLengths = []
        self._activeRequests = set()

    def log(self, request):
        "Do nothing but increment the request count; we don't need request logging."
        self.requestCount += 1
        if request.code == INTERNAL_SERVER_ERROR:
            self.errorCount += 1

    def readRequestCount(self):
        "Return and reset the request count and request error percentage."
        errorPercentage = 0
        if self.requestCount:
            errorPercentage = self.errorCount / self.requestCount * 100
        ret = self.requestCount, errorPercentage
        self.requestCount = self.errorCount = 0
        return ret

    def readEndpointRequestLengths(self):
        "Return and reset the per-endpoint request lengths."
        ret = self.endpointRequestLengths
        self.endpointRequestLengths = collections.defaultdict(list)
        return ret

    def readRequestLengths(self):
        "Return and reset the request lengths."
        ret, self.requestLengths = self.requestLengths, []
        return ret

    def getResourceFor(self, request):
        "Fetch a resource and measure the time until it's finished writing."
        resource = Site.getResourceFor(self, request)
        if self.endpointFunc is None:
            endpoint = None
        else:
            endpoint, _ = self.endpointFunc(request.path)
        request.notifyFinish().addBoth(
            self._recordRequestTime, time.time(), endpoint, request)
        self._activeRequests.add(request)
        return resource

    def _recordRequestTime(self, ign, startedAt, endpoint, request):
        "Record that a request has completed."
        delta = time.time() - startedAt
        self.requestLengths.append(delta)
        if endpoint is not None:
            self.endpointRequestLengths[endpoint].append(delta)
        self._activeRequests.discard(request)

    def gotClient(self):
        "A new client has connected."
        self.activeClients += 1

    def lostClient(self):
        "A client has disconnected."
        self.activeClients -= 1
        if not self.activeClients and self._clientsStoppedDeferred:
            self._clientsStoppedDeferred.callback(None)

    def gracefullyStopActiveClients(self):
        "Returns a Deferred that fires when all clients have disconnected."
        if not self.activeClients:
            return defer.succeed(None)
        for request in self._activeRequests:
            request.setHeader('connection', 'close')
        self._clientsStoppedDeferred = defer.Deferred()
        return self._clientsStoppedDeferred


class TryChildrenBeforeLeaf(Resource):
    "A Resource given a leaf resource tried after all of the put children."

    def __init__(self, leaf):
        Resource.__init__(self)
        self.leaf = leaf

    def getChild(self, child, request):
        "getChildWithDefault failed, so delegate to our leaf."
        request.postpath.insert(0, request.prepath.pop())
        return self.leaf

    def render(self, request):
        "Delegate requests to the root to the leaf as well."
        return self.leaf.render(request)


class FetchRequestStats(amp.Command):
    "A command to request request statistics from a running polecat server."
    arguments = []
    response = [
        ('requestCount', amp.Integer()),
        ('errorPercentage', amp.Float()),
    ]

class FetchRequestLengthStats(amp.Command):
    "A command to request request length statistics from a running polecat server."
    arguments = [
        ('percentiles', amp.ListOf(amp.Integer())),
        ('lengths', amp.ListOf(amp.Float())),
    ]
    response = [
        ('lengths', amp.ListOf(amp.Float(), optional=True)),
        ('percentiles', amp.ListOf(amp.Float(), optional=True)),
    ]

class FetchEndpointRequestLengthStats(amp.Command):
    "A command to request per-endpoint request length statistics from a running polecat server."
    arguments = []
    response = [
        ('endpoints', amp.AmpList([
            ('endpoint', amp.String()),
            ('lengths', amp.ListOf(amp.Float())),
        ])),
    ]

class FetchThreadPoolStats(amp.Command):
    "A command to request thread pool statistics from a running polecat server."
    arguments = []
    response = [
        ('threadsWaiting', amp.Integer()),
        ('threadsWorking', amp.Integer()),
        ('workerQueueSize', amp.Integer()),
    ]


class PolecatSiteStats(amp.AMP):
    @FetchRequestStats.responder
    def fetchRequestStats(self):
        requestCount, errorPercentage = self.factory.site.readRequestCount()
        return dict(
            requestCount=requestCount,
            errorPercentage=errorPercentage,
        )

    @FetchRequestLengthStats.responder
    def fetchRequestLengthStats(self, percentiles, lengths):
        requestLengths = self.factory.site.readRequestLengths()
        if not requestLengths:
            return {}
        requestLengths.sort()
        denom = len(requestLengths) / 100
        return dict(
            lengths=[percentile(requestLengths, p) for p in percentiles],
            percentiles=[
                bisect_left(requestLengths, l) / denom for l in lengths],
        )

    @FetchEndpointRequestLengthStats.responder
    def fetchEndpointRequestLengthStats(self):
        requestLengths = self.factory.site.readEndpointRequestLengths()
        endpoints = [
            dict(endpoint=k, lengths=v) for k, v in requestLengths.iteritems()]
        return dict(endpoints=endpoints)

    @FetchThreadPoolStats.responder
    def fetchThreadPoolStats(self):
        pool = self.factory.threadPool
        return dict(
            threadsWaiting=len(pool.waiters),
            threadsWorking=len(pool.working),
            workerQueueSize=pool.q.qsize(),
        )

    def makeConnection(self, transport):
        "Upcall to avoid noisy logging messages on connection open."
        amp.BinaryBoxProtocol.makeConnection(self, transport)

    def connectionLost(self, reason):
        "Upcall to avoid noisy logging messages on connection close."
        amp.BinaryBoxProtocol.connectionLost(self, reason)
        self.transport = None


class PolecatSiteStatsFactory(Factory):
    protocol = PolecatSiteStats
    noisy = False

    def __init__(self, site, threadPool):
        self.site = site
        self.threadPool = threadPool
