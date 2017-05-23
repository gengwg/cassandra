#!/usr/bin/env python3
# A crm cassandra client.

from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import dict_factory, tuple_factory, \
    named_tuple_factory, BatchStatement, SimpleStatement
import json
import hashlib
import configparser
import datetime
import sys
import os


class CassandraClient(object):
    def __init__(self):
        config = configparser.RawConfigParser()
        config.read(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'cassandra.properties'))
        keyspace = config.get('DatabaseSection','database.dbname')
        timeout = config.get('DatabaseSection','database.sessiontimeout')
        uname = config.get('DatabaseSection','database.username')
        passwd = config.get('DatabaseSection','database.password')
        nodelist = config.get('DatabaseSection','database.nodelist')
        nodes = nodelist.split(',')
        # print keyspace,timeout,uname,passwd,nodelist,nodes
        _credentials = PlainTextAuthProvider(username=uname, password=passwd)
        self.cluster = Cluster(nodes,
                          protocol_version=3,
                          auth_provider=_credentials,
                          load_balancing_policy=DCAwareRoundRobinPolicy(local_dc=''))
        self.session = self.cluster.connect(keyspace)
        self.session.row_factory = named_tuple_factory
        #self.session.row_factory = dict_factory
        self.session.default_timeout = int(timeout)
        self.wlist = 'campaign_wlists'

    def add_to_wlist(self, *, business_unit, type, campaign_name):
        """Insert one campaign into the white list"""

        prepared_stmt = self.session.prepare(
            "INSERT INTO {} (business_unit, type, campaign_name) VALUES (?, ?, ?)".format(self.wlist))
        bound_stmt = prepared_stmt.bind([business_unit, type, campaign_name])
        self.session.execute(bound_stmt)


    def close_session(self):
        if self.session:
            self.session.shutdown()
        if self.cluster:
            self.cluster.shutdown()

if __name__ == '__main__':
    client = CassandraClient()
    client.add_to_wlist(business_unit="abc", type="def", campaign_name="test_campaign123")
    client.close_session()
