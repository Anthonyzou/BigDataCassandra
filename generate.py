import ast
import cassandra
from cassandra.cluster import Cluster
import random, sys
import string
import timeit


def randword(length):
    return ''.join(random.choice(string.lowercase) for i in range(length))

acluster = 0
datadate = 1385305327 #equal to 2013-11-24 08:02:07-0700
session = None
def generate(label, element_type, frequency):
    global acluster
    global session
    global datadate
    """ Generate an element value to insert. of arbitrary type, which is a null
    value (1000-frequency)/1000 of the time
    """
    if (random.randint(0, 1000) <= frequency):
        # column-specific data
        if (label == "MOBILE_ID_TYPE"):  # pretend this is partition by cluster
            result = acluster % 8
            session.execute_async(
                    session.prepare("update group_by_MOBILE_ID_TYPE set count = count + 1  where MOBILE_ID_TYPE = ?")
                    .bind([result]))
            acluster += 1
        elif (label == "MONTH_DAY"):
            result = random.randint(1, 31)
            session.execute_async(
                    session.prepare("update group_by_month set count = count + 1  where MONTH_DAY = ?")
                    .bind([result]))
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
    else:
        result = None
    return result


if __name__ == '__main__':
    start_time = timeit.default_timer()
    cluster = Cluster(
        ['199.116.235.57', '10.0.0.31', '10.0.0.38', '127.0.0.1'], port=9233)
    session = cluster.connect('group3')  # keyspace should be our own
    # CREATE KEYSPACE group3 WITH REPLICATION = { 'class' : 'SimpleStrategy','replication_factor' : 1 }
    print cluster.metadata.cluster_name
    print cassandra.__version__ +"\n"

    random.seed(3333)
    with open("cdr_table.sql") as tables_setup:
        cols = tables_setup.read()
        for truncate in ["truncate cdr",  "truncate query3",  "truncate group_by_month",  "truncate group_by_MOBILE_ID_TYPE"
                         ,"CREATE TABLE cdr(" + cols + """primary key(MSC_CODE ,CITY_ID,SERVICE_NODE_ID,RUM_DATA_NUM ,
                            MONTH_DAY ,DUP_SEQ_NUM ,MOBILE_ID_TYPE ,SEIZ_CELL_NUM ,FLOW_DATA_INC ,SUB_HOME_INT_PRI ,
                            CON_OHM_NUM)) with clustering order by (city_id asc)"""
                         ,"CREATE TABLE query3(" + cols + """primary key(MSC_CODE ,MOBILE_ID_TYPE ,CITY_ID,SERVICE_NODE_ID,RUM_DATA_NUM ,
                            MONTH_DAY ,DUP_SEQ_NUM ,SEIZ_CELL_NUM ,FLOW_DATA_INC ,SUB_HOME_INT_PRI ,
                            CON_OHM_NUM)) with clustering order by (MOBILE_ID_TYPE asc)"""
                          ,"Create table group_by_month (MONTH_DAY int, count counter, primary key (month_day))"
                          ,"Create table group_by_MOBILE_ID_TYPE (MOBILE_ID_TYPE int, count counter, primary key (MOBILE_ID_TYPE))"]:
            try:
                session.execute(truncate, timeout=None)
            except Exception as error:
                pass

    # read table stuffs from sample table schema
    with open("tablestuffs.txt") as tables_freq:
        (labels, counts) = tables_freq
    labels = ast.literal_eval(labels)  # turn input into list correctly
    counts = ast.literal_eval(counts)  # turn input into list correctly
    types = []

    body = ""
    # build columns
    for i in labels:
        body += i[0] + ","
        types.append(i[1])
    body = body[:-1] + ") VALUES (" + ("?," * (len(labels) - 1)) + "?)"
    # remove last char
    # build question marks for binding
    prepared = session.prepare("INSERT INTO cdr ("+ body )
    prepared2 = session.prepare("INSERT INTO query3 (" + body)
    print( "query built and prepared")
    
    days = 30
    entriesPerDay = 300
    optimize = False
    try:
        if int(sys.argv[1]) >= 1:
            days = int(sys.argv[1])
    except:
        pass
    try:
        if sys.argv[2] > 1:
            optimize = True
    except:
        pass
    # example async insert into table
    try:
        for day in range(days):
            for entry in range(entriesPerDay):
                if entry == entriesPerDay:
                    datadate += 86400 #increment one day 
                build = []
                for x in range(len(labels)):
                    build.append(generate(labels[x][0], types[x], counts[x]))
                session.execute_async( prepared.bind(build))
                if optimize:
                    session.execute_async( prepared2.bind(build))
    except Exception as error:
        print error

    print str((timeit.default_timer() - start_time)/60), " minutes elapsed"
