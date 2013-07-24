# -*- python -*-
# Copyright (c) Aaron Gallagher <_@habnab.it>
# See COPYING for details.

import sqlite3

from twisted.application.internet import TimerService, TCPClient
from twisted.application.service import Application

from stat_collection import ClientPoller, ReconnectingAMPFactory


application = Application('stat-collection')
database = sqlite3.connect('stats.sqlite')
poller = ClientPoller(database)

servers = [
    ('localhost', 8267),
    ('localhost', 8267),
]

for host, port in servers:
    fac = ReconnectingAMPFactory(poller.clients)
    TCPClient(host, port, fac).setServiceParent(application)
TimerService(10, poller.poll).setServiceParent(application)
