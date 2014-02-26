from cassandra.cluster import Cluster
import random
from random import randint
from xmlrpclib import MAXINT, MININT

cluster = Cluster(['199.116.235.57',
                   '10.0.0.31',
                   '10.0.0.38',
                   '127.0.0.1'
                   ])

session = cluster.connect('group3')  # keyspace should be our own

metadata = cluster.metadata
print metadata.cluster_name  # should make sure this is group3

random.seed(3333)

# drop table if it already exists
session.execute("drop table call_details_record")

with open("../misc/bigdata_setup1.sql") as tables_setup:
    session.execute(tables_setup.read())

# read table stuffs from sample table schema
with open("tablestuffs.txt") as tables_freq:
    (labels, counts) = tables_freq

query = "INSERT INTO call_details_record"
for i in labels:
    query += i[0]
query += "VALUES ("
for i in range(len(labels) - 1):
    query += "?, "
query += "? )"
prepared = session.prepare(query)

# example async insert into table
for y in range(10):
    build = []
    for x in range(len(counts)):
        to_insert = randint(1, MAXINT) \
            if (randint(1, 1000) < counts[x]) else None
        build.append(to_insert)
    bound = (prepared.bind(build))
    session.execute_async(bound)
