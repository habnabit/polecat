# -*- python -*-

import functools

from twisted.application.internet import StreamServerEndpointService
from twisted.application import service
from twisted.internet import reactor, endpoints
from twisted.web.wsgi import WSGIResource
from twisted.web import static

import polecat
import yourapp


threadPool = reactor.getThreadPool()
wsgiResource = WSGIResource(reactor, threadPool, yourapp.wsgi_app)
rootResource = polecat.TryChildrenBeforeLeaf(wsgiResource)
rootResource.putChild('static', static.File('static'))

endpointFunc = functools.partial(yourapp.app._match, yourapp.app.mapping)
site = polecat.PolecatSite(rootResource, endpointFunc=endpointFunc)
siteStats = polecat.PolecatSiteStatsFactory(site, threadPool)
reactor.addSystemEventTrigger('before', 'shutdown', site.gracefullyStopActiveClients)

application = service.Application('example-webapp')
def attachServerEndpoint(factory, description):
    "Generates a server endpoint from a string and attaches it to the application."
    endpoint = endpoints.serverFromString(reactor, description)
    StreamServerEndpointService(endpoint, factory).setServiceParent(application)

attachServerEndpoint(site, 'tcp:8080:interface=127.0.0.1')
attachServerEndpoint(siteStats, 'tcp:8267:interface=127.0.0.1')
