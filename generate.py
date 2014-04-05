import ast
import cassandra
from cassandra.cluster import Cluster
import random, sys
import string
import timeit
import uuid,os
import pycassa

def randword(length):
    return ''.join(random.choice(string.lowercase) for i in range(length))

# read table stuffs from sample table schema
with open("tableFrequency.txt") as tables_freq:
    (labels, counts) = tables_freq
labels = ast.literal_eval(labels)  # turn input into list correctly
counts = ast.literal_eval(counts)  # turn input into list correctly
acluster = 0
datadate = 1385305327 #equal to 2013-11-24 08:02:07-0700
session = None
def generate():
    global acluster
    global session
    global datadate
    global labels
    global counts
    """ Generate an element value to insert. of arbitrary type, which is a null
    value (1000-frequency)/1000 of the time
    """
    build = dict()
    for x in range(len(labels)):
            # column-specific data
        if (labels[x] == "MOBILE_ID_TYPE"):  # pretend this is partition by cluster
            build.update({labels[x][0]: str(acluster % 8)})
            acluster+=1
        elif (labels[x] == "MONTH_DAY"):
            build.update({labels[x][0]: str(random.randint(1, 31))})
        elif labels[x][1] == 'text':
            build.update({labels[x][0]: randword(10)})
        elif(labels[x][0] != "SEQ_NUM" and labels[x][1] == 'int'):
            build.update({labels[x][0]: randword(10)})
    return build
if __name__ == '__main__':
    start_time = timeit.default_timer()
    session = Cluster(['10.0.0.31', '10.0.0.38', '127.0.0.1'], 
                      port=9233).connect()  # keyspace should be our own
    
    print "\n",cassandra.__version__,"\n"
    
    seed = os.urandom(3)
    try:
        seed = int(sys.argv[2])
        session.execute("use group3")
    except:
        session.execute("drop keyspace if exists group3", timeout=None)
        session.execute("CREATE KEYSPACE group3 WITH REPLICATION = { 'class' : 'SimpleStrategy','replication_factor' : 1 }",timeout=None)
        session.execute("use group3")
        with open("tableColumns.sql") as tables_setup:
            cols = tables_setup.read()
            for setupcmd in ["CREATE TABLE cdr(" + cols + ") with compact storage"
                             ,"""Create table group_by_month ( MONTH_DAY int, count counter, primary key (MONTH_DAY)) 
                               with compact storage and compression={ 'sstable_compression':''}"""
                             ,"Create table group_by_MOBILE_ID_TYPE (MOBILE_ID_TYPE int, count counter, primary key (MOBILE_ID_TYPE))  with compact storage"]:
                try:
                    session.execute(setupcmd, timeout=None)
                except Exception as error:
                    print setupcmd
                    print error

    random.seed(seed)
    
    pool = pycassa.ConnectionPool(keyspace='group3', server_list=['127.0.0.1:9133'],)
    cdr = pycassa.ColumnFamily(pool, 'cdr')
    
    entriesPerDay = 100000
    try: days = int(sys.argv[1])    
    except: days = 10
    
    b = cdr.batch(queue_size=100,atomic=True)
    # example async insert into table
    for day in range(days):
        for entry in range(entriesPerDay):
            b.insert((random.randint(-90000000,90000000)), generate())
            if entry == entriesPerDay:
                datadate += 86400 #increment one day
    
    print str((timeit.default_timer() - start_time)/60), " minutes elapsed"
    print seed, "seed used", days, 'days generated'
