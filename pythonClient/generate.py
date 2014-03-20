import ast
import cassandra
from cassandra.cluster import Cluster
from datetime import date
import random
import string
import time

acluster = 0


def randword(length):
    return ''.join(random.choice(string.lowercase) for i in range(length))


def generate(label, element_type, frequency):
    global acluster
    """ Generate an element value to insert. of arbitrary type, which is a null
    value (1000-frequency)/1000 of the time
    """
    if (random.randint(0, 1000) < frequency):
        # column-specific data
        if (label == "MSC_CODE"):  # pretend this is partition by cluster
            result = acluster % 8
            acluster += 1
        elif (label == "MONTH_DAY"):
            result = random.randint(1, 31)
        elif (label == "LONGITUDE" or label == "LAST_LONGITUDE"):
            result = random.random() * 360 - 180
        elif (label == "LATITUDE" or label == "LAST_LATITUDE"):
            result = random.random() * 180 - 90
        # type-specific fallback data
        elif (element_type == "float"):
            result = random.random()
        elif (element_type == "text"):
            result = randword(16)
        elif (element_type == "timestamp"):
            result = random.randint(1385305327, 1395305327)
        else:
            result = random.randint(0, 1000000)
    else:
        result = None
    return result


if __name__ == '__main__':
    start_time = time.clock()
    cluster = Cluster(
        ['199.116.235.57', '10.0.0.31', '10.0.0.38', '127.0.0.1'], port=9233)
    session = cluster.connect('group3')  # keyspace should be our own
    # CREATE KEYSPACE group3 WITH REPLICATION = { 'class' : 'SimpleStrategy',
    # 'replication_factor' : 1 }
    print cluster.metadata.cluster_name  # should make sure this is group3
    print cassandra.__version__

    random.seed(3333)
    try:
        session.execute("drop table cdr")
    except:
        pass
    try:
        with open("cdr_table.sql") as tables_setup:
            cols = tables_setup.read()
            session.execute(
                "CREATE TABLE cdr(" + cols + """primary key(MSC_CODE ,CITY_ID,
                PCMD_VER  ,
                SEQ_NUM ,
                MONTH_DAY ,
                DUP_SEQ_NUM ,
                MOBILE_ID_TYPE ,
                SESS_REQ_TYPE ,
                SESS_SFC ,
                SESS_OR_CONN_CPFAIL ,
                CFC)) with clustering order by (city_id asc)""")
            session.execute("CREATE INDEX seq_num_index ON cdr (MONTH_DAY)")
    except Exception as error:
        print error
        # exit()
        # remove table informations if it already exists

    # read table stuffs from sample table schema
    with open("tablestuffs.txt") as tables_freq:
        (labels, counts) = tables_freq
    labels = ast.literal_eval(labels)  # turn input into list correctly
    counts = ast.literal_eval(counts)  # turn input into list correctly
    types = []

    query = "INSERT INTO cdr ("
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
    for y in range(30000):
        build = []
        for x in range(len(labels)):
            #print("counts: " + str(counts[x]))
            thing = generate(labels[x][0], types[x], counts[x])
            build.append(thing)
        bound = prepared.bind(build)
        session.execute_async(bound)

    print str((time.clock() - start_time) / 60)[:7], "minutes elapsed"
