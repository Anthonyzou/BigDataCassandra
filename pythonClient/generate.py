import ast
import random
import time
from datetime import date
from xmlrpclib import MAXINT

import cassandra
from cassandra.cluster import Cluster


def generate(element_type, frequency):
    """ Generate an element value to insert. of arbitrary type, which is a null
    value (1000-frequency)/1000 of the time
    """
    if (random.randint(0, 1000) < frequency):
        if (element_type == "float"):
            result = random.random()
        elif (element_type == "text"):
            result = "\"I'm a string\""
        elif (element_type == "timestamp"):
            MAXTIME = time.time()
            timeresult = date.fromtimestamp(random.randint(0, MAXTIME))
            result = timeresult.isoformat()
        else:
            result = random.randint(0, MAXINT)
    return result

start_time = time.clock()

cluster = Cluster(['199.116.235.57', '10.0.0.31', '10.0.0.38', '127.0.0.1'])

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
labels = ast.literal_eval(labels)  # turn input into list correctly
types = []

query = "INSERT INTO call_details_record ("
# build columns
for i in labels:
    query += i[0] + ","
    types.append(i[1])

# remove last char
# build question marks for binding
prepared = session.prepare(
    query[:-1] + ") VALUES (" + ("?," * (len(labels) - 1)) + "?)")
print "query built and prepared"

# example async insert into table
for y in range(0, 10000):
    build = []
    for x in range(len(labels)):
        build.append(generate(types[x], counts[x]))
    bound = prepared.bind(build)
    session.execute_async(bound)

print str((time.clock() - start_time) / 60)[:7], "minutes elapsed"
