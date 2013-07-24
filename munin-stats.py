#!/usr/bin/env python
#%# family=auto
#%# capabilities=autoconf suggest

from twisted.internet.endpoints import clientFromString
from twisted.internet import defer, task, protocol
from twisted.protocols import amp

import os
import polecat

PERCENTILES = [0, 50, 85, 95, 98, 100]
LENGTHS = [0.1, 0.5, 1, 2, 5]


def labelify(x):
    return str(x).replace('.', '_')


pluginClasses = []
def addPlugin(cls):
    # makes a class attribute instead of a metaclass attribute.
    cls.name = cls.__name__
    pluginClasses.append(cls)
    return cls


@addPlugin
class polecat_request(object):
    def __init__(self, endpoints):
        self.endpoints = endpoints

    def suggest(self):
        return ['']

    def command_autoconf(self, ignored):
        print 'yes'

    @defer.inlineCallbacks
    def fetchStats(self, endpoint):
        amp = yield endpoint.connect(AMPFactory())
        stats = yield amp.callRemote(polecat.FetchRequestStats)
        defer.returnValue(stats)

    @defer.inlineCallbacks
    def command_fetch(self, ignored):
        names, endpoints = zip(*self.endpoints.iteritems())
        results = yield defer.DeferredList(
            [self.fetchStats(endpoint) for endpoint in endpoints],
            consumeErrors=True)
        resultsByParameter = {}
        for name, (success, stats) in zip(names, results):
            if not success:
                continue
            for k, v in stats.iteritems():
                resultsByParameter.setdefault(k, {})[name] = v

        print 'multigraph polecat_request'
        for name, v in resultsByParameter['requestCount'].iteritems():
            print 'requestCount_%s.value %s' % (name, v)

        print 'multigraph polecat_error_percentage'
        for name, v in resultsByParameter['errorPercentage'].iteritems():
            print 'errorPercentage_%s.value %s' % (name, v)

    def command_config(self, ignored):
        print """
multigraph polecat_request
graph_title Polecat requests
graph_category polecat
graph_args --base 1000
graph_vlabel Requests per second
"""

        for name in sorted(self.endpoints):
            print """
requestCount_{0}.type ABSOLUTE
requestCount_{0}.min 0
requestCount_{0}.draw LINESTACK2
requestCount_{0}.label {0} requests
requestCount_{0}.info The number of requests to the polecat server
""".format(name)

        print """
multigraph polecat_error_percentage
graph_title Polecat request error percentage
graph_category polecat
graph_args --base 1000
graph_vlabel request error %
"""

        for name in sorted(self.endpoints):
            print """
errorPercentage_{0}.type ABSOLUTE
errorPercentage_{0}.min 0
errorPercentage_{0}.max 100
errorPercentage_{0}.draw LINE2
errorPercentage_{0}.label {0} request error percentage
errorPercentage_{0}.info What percentage of requests resulted in a 500 status code
""".format(name)


class PerEndpointPluginBase(object):
    command = None

    def __init__(self, endpoints):
        self.endpoints = endpoints

    def _ensureEndpoint(self, name):
        if name not in self.endpoints:
            raise ValueError("%r is not a known client" % (name,))

    def suggest(self):
        return self.endpoints

    def command_autoconf(self, ignored):
        print 'yes'

    @defer.inlineCallbacks
    def command_fetch(self, endpointName):
        self._ensureEndpoint(endpointName)
        endpoint = self.endpoints[endpointName]
        amp = yield endpoint.connect(AMPFactory())
        stats = yield self.fetchStats(amp)
        self.formatStats(stats, endpointName)

    def fetchStats(self, amp):
        return amp.callRemote(self.command)

    def formatStats(self, stats, endpointName):
        for k, v in stats.iteritems():
            if v is None:
                v = 'U'
            print '%s.value %s' % (k, v)


@addPlugin
class polecat_request_length_(PerEndpointPluginBase):
    command = polecat.FetchRequestLengthStats

    def fetchStats(self, amp):
        return amp.callRemote(
            self.command, percentiles=PERCENTILES, lengths=LENGTHS)

    def formatStats(self, stats, endpointName):
        if stats['lengths'] is not None:
            print 'multigraph polecat_request_length_%s' % endpointName
            for p, v in zip(PERCENTILES, stats['lengths']):
                print 'percentile_%s.value %s' % (p, v)

        if stats['percentiles'] is not None:
            print 'multigraph polecat_request_percentile_%s' % endpointName
            for l, v in zip(LENGTHS, stats['percentiles']):
                print 'length_%s.value %s' % (labelify(l), v)

    def command_config(self, endpointName):
        self._ensureEndpoint(endpointName)
        print """
multigraph polecat_request_length_{0}
graph_title {0} request processing length
graph_category polecat
graph_args --base 1000 -l 0
graph_vlabel s
""".format(endpointName)

        for p in PERCENTILES:
            print """
percentile_{0}.type GAUGE
percentile_{0}.min 0
percentile_{0}.label {0}th percentile request length
percentile_{0}.info The {0}th percentile of time taken processing the request since the last poll
""".format(p)

        print """
multigraph polecat_request_percentile_{0}
graph_title {0} percentile of request processing length
graph_category polecat
graph_args --base 1000 -u 100 --rigid
graph_vlabel percentile
""".format(endpointName)

        for l in LENGTHS:
            print """
length_{0}.type GAUGE
length_{0}.min 0
length_{0}.max 100
length_{0}.label percentile of {1}s request
length_{0}.info The percentile of a {1}s request during the last poll interval
""".format(labelify(l), l)


@addPlugin
class polecat_threadpool_(PerEndpointPluginBase):
    command = polecat.FetchThreadPoolStats

    def command_config(self, endpointName):
        self._ensureEndpoint(endpointName)
        print """
graph_title {0} thread pool
graph_category polecat
graph_args --base 1000 -l 0
graph_vlabel Number of threads
graph_order threadsWaiting threadsWorking workerQueueSize
threadsWaiting.type GAUGE
threadsWaiting.min 0
threadsWaiting.draw AREASTACK
threadsWaiting.label threads waiting
threadsWaiting.info The number of threads waiting for work
threadsWorking.type GAUGE
threadsWorking.min 0
threadsWorking.draw AREASTACK
threadsWorking.label threads working
threadsWorking.info The number of threads doing work
workerQueueSize.type GAUGE
workerQueueSize.min 0
workerQueueSize.draw AREASTACK
workerQueueSize.label jobs pending
workerQueueSize.info The number of jobs waiting for a free thread
""".format(endpointName)


class AMPFactory(protocol.ClientFactory):
    protocol = amp.AMP

def nameLength(cls):
    return len(cls.__name__)

def main(reactor, procName, *args):
    clientEndpoints = {}
    for k, v in os.environ.iteritems():
        _, _, clientName = k.partition('client_endpoint_')
        if clientName:
            clientEndpoints[clientName] = clientFromString(reactor, v)
    if not clientEndpoints:
        raise ValueError("no client endpoints detected in the environment")

    plugins = [pluginClass(clientEndpoints)
               for pluginClass in sorted(pluginClasses, key=nameLength, reverse=True)]

    if args == ('suggest',):
        suggestions = []
        for plugin in plugins:
            suggestions.extend(plugin.name + arg for arg in plugin.suggest())
        print '\n'.join(suggestions)
        return defer.succeed(None)

    procName = os.path.basename(procName)
    for plugin in plugins:
        _, foundPluginName, arg = procName.partition(plugin.name)
        if not foundPluginName:
            continue
        command = 'fetch' if not args else args[0]
        method = getattr(plugin, 'command_' + command, None)
        if not method:
            raise ValueError("%r plugin can't handle the command %r" % (plugin.name, command))
        return defer.maybeDeferred(method, arg)

    raise ValueError("no plugin was found with the name %r" % (procName,))


import sys
task.react(main, sys.argv)
