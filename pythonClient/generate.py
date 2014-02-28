from cassandra.cluster import Cluster
import time, ast, random
from random import randint
from xmlrpclib import MAXINT, MININT
import cassandra

start_time = time.clock()

cluster = Cluster(['199.116.235.57',
                   '10.0.0.31',
                   '10.0.0.38',
                   '127.0.0.1'
                   ])

session = cluster.connect('group3')  # keyspace should be our own
print cluster.metadata.cluster_name  # should make sure this is group3

print cassandra.__version__

random.seed(3333)

try:
    with open("../misc/bigdata_setup1.sql") as tables_setup:
        session.execute(tables_setup.read())
except:
    # remove table informations if it already exists
    session.execute("truncate call_details_record")

# read table stuffs from sample table schema
with open("tablestuffs.txt") as tables_freq:
    (labels, counts) = tables_freq
labels = ast.literal_eval(labels) #turn input into list correctly
types = []

query = "INSERT INTO call_details_record ("
# build columns
for i in labels:
    query += i[0]+","
    types.append(i[1])

# remove last char 
# build question marks for binding
prepared = session.prepare(query[:-1] + ") VALUES (" + ("?, " * (len(labels)-1)) +  "? )")
print "query built and prepared"

# example async insert into table
for y in range(0,100):
    build = []
    for x in range(len(labels)):
        if types[x] == 'bigint':
            build.append(y)
        elif types[x] not in ['int', 'float']:
            build.append(None)
        else:
            build.append(randint(1, MAXINT)) \
            if (randint(0, 1000) < counts[x]) else build.append(None)
            
    bound = prepared.bind(build)
    session.execute_async(bound)

print str((time.clock() - start_time)/60)[:7], "minutes elapsed"


