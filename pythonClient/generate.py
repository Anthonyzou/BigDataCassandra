import ast
import cassandra
from cassandra.cluster import Cluster
from datetime import date
import random
import time


def generate(label,element_type, frequency):
    """ Generate an element value to insert. of arbitrary type, which is a null
    value (1000-frequency)/1000 of the time
    """
    if (random.randint(0, 1000) < frequency):
        if (element_type == "float"):
            result = None #random.random()
        elif(label[0] == "MSC_CODE"):#pretend this is partition by cluster
            result = random.randint(0,8)
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
        session.execute("CREATE TABLE cdr(" + cols + """primary key((MSC_CODE) ,CITY_ID,
                         PCMD_VER  ,
                         SEQ_NUM ,
                         MONTH_DAY ,
                         DUP_SEQ_NUM ,
                         MOBILE_ID_TYPE ,
                         SESS_REQ_TYPE ,
                         SESS_SFC ,
                         SESS_OR_CONN_CPFAIL ,CFC)) with clustering order by (city_id asc)""")
except Exception as error:
    print error
    #exit()
    # remove table informations if it already exists
try:
    session.execute("truncate cdr")
except Exception as error :
    print error

# read table stuffs from sample table schema
with open("tablestuffs.txt") as tables_freq:
    (labels, counts) = tables_freq
labels = ast.literal_eval(labels)  # turn input into list correctly
types = []

query = "INSERT INTO query1_2_3 ("
# build columns
for i in labels:
    query += i[0] + ","
    types.append(i[1])

# remove last char
# build question marks for binding
prepared = session.prepare(query[:-1] + ") VALUES (" + ("?," * (len(labels) - 1)) + "?)")
print "query built and prepared"

# example async insert into table
for y in range(0, 10000):
    build = []
    for x in range(len(labels)):
        thing = generate(labels[x], types[x], counts[x])
        build.append(thing)
    bound = prepared.bind(build)
    session.execute_async(bound)

print str((time.clock() - start_time) / 60)[:7], "minutes elapsed"



