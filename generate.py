import ast
import cassandra
from cassandra.cluster import Cluster
import random, sys
import string
import timeit
import uuid

def randword(length):
    return ''.join(random.choice(string.lowercase) for i in range(length))

acluster = 0
datadate = 1385305327 #equal to 2013-11-24 08:02:07-0700
session = None
SEIZ_CELL_NUM_L = None
def generate(label, element_type, frequency):
    global acluster, session, datadate, SEIZ_CELL_NUM_L

    # column-specific data
    if (label == "MOBILE_ID_TYPE"):  # pretend this is partition by cluster
        result = acluster % 8
        session.execute_async(
                session.prepare("insert into group_by_MOBILE_ID_TYPE (MOBILE_ID_TYPE, id) values (?,?)")
                .bind([result, SEIZ_CELL_NUM_L]))
        acluster += 1
    elif label == "SEIZ_CELL_NUM_L":
        result = uuid.uuid4()
        SEIZ_CELL_NUM_L = result
    elif (label == "MONTH_DAY"):
        result = random.randint(1, 31)
        session.execute_async(
                session.prepare("insert into group_by_month (MONTH_DAY, id) values (?,?)")
                .bind([result, SEIZ_CELL_NUM_L]))
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
        result = datadate
    else:
        result = random.randint(0, 9900000)
    return result


if __name__ == '__main__':
    start_time = timeit.default_timer()
    cluster = Cluster(['10.0.0.31', '10.0.0.38', '127.0.0.1'], port=9233)
    session = cluster.connect() 
    
    print cluster.metadata.cluster_name # cluster should be our own
    print cluster.cql_version
    print cassandra.__version__ ,"\n"
    
    seed = 3333
        # read table stuffs from sample table schema
    with open("tableFrequency.txt") as tables_freq:
        (labels, counts) = tables_freq
    labels = ast.literal_eval(labels)  # turn input into list correctly
    counts = ast.literal_eval(counts)  # turn input into list correctly
    
    try:
        seed = int(sys.argv[2])
        session.set_keyspace("group3")
    except: 
        session.execute("drop keyspace if exists group3", timeout=None)
        session.execute("""CREATE KEYSPACE group3 WITH REPLICATION = { 'class' : 'SimpleStrategy','replication_factor' : 3 } 
                            """,timeout=None)
        session.set_keyspace("group3")
        with open("tableColumns.sql") as tables_setup:
            cols = tables_setup.read()
            for setupcmd in ["CREATE TABLE cdr(" + cols + "primary key(" + "".join(labels[i][0]+"," for i in range(100))[:-1] + """))
                               with compression={ 'sstable_compression':''}"""
                              ,"Create table group_by_month (MONTH_DAY int, id uuid, primary key (month_day, id))"
                              ,"Create table group_by_MOBILE_ID_TYPE (MOBILE_ID_TYPE int, id uuid, primary key (MOBILE_ID_TYPE,id))"
                              , "create index on cdr (LONGITUDE)", "create index on cdr (LATITUDE)"
                               ]:
                try:
                    session.execute(setupcmd, timeout=None)
                except Exception as error:
                    print error
    random.seed(seed)

    
    body = "".join(i[0]+"," for i in labels)[:-1] + ") VALUES (" + ("?," * (len(labels) - 1)) + "?)"
    # remove last char
    # build question marks for binding
    prepared = session.prepare("INSERT INTO cdr ("+ body )
    
    print( "query built and prepared")
    
    try: days = int(sys.argv[1])
    except: days = 1
    entriesPerDay = 10000
    # example async insert into table
    for day in range(days):
        for entry in range(entriesPerDay):
            if entry == entriesPerDay:
                datadate += 86400 #increment one day 
            build = []
            for x in range(len(labels)):
                build.append(generate(labels[x][0], labels[x][1], counts[x]))
            try:
                session.execute_async( prepared.bind(build))
            except:
                pass

    print str((timeit.default_timer() - start_time)/60), " minutes elapsed"
    print seed, "seed used", days, 'days generated'