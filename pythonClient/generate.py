import ast
import cassandra
from cassandra.cluster import Cluster
from datetime import date
import random
import time


def generate(element_type, frequency):
    """ Generate an element value to insert. of arbitrary type, which is a null
    value (1000-frequency)/1000 of the time
    """
    if (random.randint(0, 1000) < frequency):
        if (element_type == "float"):
            result = None #random.random()
        elif (element_type == "text"):
            result = None #"\"I'm a string\""
        elif (element_type == "timestamp"):
#             MAXTIME = time.time()
#             timeresult = date.fromtimestamp(random.randint(0, MAXTIME))
#             result = timeresult.isoformat()
            result = None
        else:
            result = random.randint(0, 100000)
    return result

start_time = time.clock()

cluster = Cluster(['199.116.235.57', '10.0.0.31', '10.0.0.38', '127.0.0.1'])

session = cluster.connect('group3')  # keyspace should be our own
# CREATE KEYSPACE group3 WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }
print cluster.metadata.cluster_name  # should make sure this is group3
print cassandra.__version__

random.seed(3333)
try:
    with open("cdr_table.sql") as tables_setup:
        cols = tables_setup.read()
        session.execute("CREATE TABLE query1_2(" + cols + " primary key(SEQ_NUM, city_id)) with clustering order by (city_id desc) ")
        #session.execute("CREATE TABLE query3("  +  cols + " primary key(SEQ_NUM,CITY_ID)) WITH CLUSTERING ORDER BY (CITY_ID DESC)")
        #session.execute("CREATE TABLE query4("  +  cols + " primary key(SEQ_NUM,CITY_ID)) WITH CLUSTERING ORDER BY (CITY_ID DESC)")
        #session.execute("CREATE TABLE query5("  +  cols + " primary key(SEQ_NUM,CITY_ID)) WITH CLUSTERING ORDER BY (CITY_ID DESC)")
        #session.execute("CREATE TABLE query6("  +  cols + " primary key(SEQ_NUM,CITY_ID)) WITH CLUSTERING ORDER BY (CITY_ID DESC)")
except Exception as error:
    print error
    #exit()
    # remove table informations if it already exists
try:
    session.execute("truncate query1_2")
    session.execute("truncate query3")
    session.execute("truncate query4")
    session.execute("truncate query5")
    session.execute("truncate query6")
except Exception as error :
    print error

# read table stuffs from sample table schema
with open("tablestuffs.txt") as tables_freq:
    (labels, counts) = tables_freq
labels = ast.literal_eval(labels)  # turn input into list correctly
types = []

query = "INSERT INTO query1_2 ("
# build columns
for i in labels:
    query += i[0] + ","
    types.append(i[1])

# remove last char
# build question marks for binding
prepared = session.prepare(query[:-1] + ") VALUES (" + ("?," * (len(labels) - 1)) + "?)")
print "query built and prepared"

# example async insert into table
for y in range(0, 10):
    build = []
    for x in range(len(labels)):
        thing = generate(types[x], counts[x])
        build.append(thing)
    bound = prepared.bind(build)
    session.execute_async(bound)

print str((time.clock() - start_time) / 60)[:7], "minutes elapsed"



