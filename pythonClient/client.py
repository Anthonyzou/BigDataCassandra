from cassandra.cluster import Cluster
import random
from random import randint
from xmlrpclib import MAXINT, MININT

cluster = Cluster( ['199.116.235.57',
                    '10.0.0.31',
                    '10.0.0.38',
                    '127.0.0.1'
                    ])

session = cluster.connect('system') #keyspace should be our own

metadata = cluster.metadata
print metadata.cluster_name

random.seed(3333)
print randint(MININT, MAXINT)
print randint(MININT, MAXINT)

query = "INSERT INTO users (id, name, age) VALUES (?, ?, ?)" 
prepared = session.prepare(query)

#example async insert into table.
for x in range(0,100):
     bound = prepared.bind((None, None, None))
     session.execute_async(bound)

    