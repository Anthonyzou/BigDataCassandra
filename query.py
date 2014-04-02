import cassandra
from cassandra.cluster import Cluster
import timeit

cluster = Cluster(['10.0.0.70', '10.0.0.31', '10.0.0.38', '127.0.0.1'], port=9233)

session = cluster.connect('group3')  # keyspace should be our own

loop = False #turn this to true when you want to do looping queries for group by

print cluster.metadata.cluster_name  # should make sure this is group3
print cassandra.__version__
print ""
program_st = timeit.default_timer()
#======================================================================
# QUERY 1
#======================================================================
query = """
SELECT count (*) as ten_atomic
FROM cdr 
WHERE 
(CITY_ID,SERVICE_NODE_ID,RUM_DATA_NUM,MONTH_DAY,DUP_SEQ_NUM,MOBILE_ID_TYPE,SEIZ_CELL_NUM,FLOW_DATA_INC,SUB_HOME_INT_PRI,CON_OHM_NUM) 
> (10000,10000,10000,3,10000,1,10000,10000,10000,10000)
AND (CITY_ID,SERVICE_NODE_ID,RUM_DATA_NUM,MONTH_DAY,DUP_SEQ_NUM,MOBILE_ID_TYPE,SEIZ_CELL_NUM,FLOW_DATA_INC,SUB_HOME_INT_PRI,CON_OHM_NUM) 
< (150000,150000,150000,30,150000,6,150000,150000,150000,150000)
LIMIT 40000000
ALLOW FILTERING ;
"""
start_time = timeit.default_timer()
result = session.execute(query, timeout=None)[0]
print result

print str((timeit.default_timer() - start_time) /60), " minutes elapsed for query 1\n"
#======================================================================
# QUERY 2
#======================================================================
query = """ 
SELECT count (*) as range_city_id
FROM cdr
WHERE
CITY_ID > 5000 AND CITY_ID < 90000
LIMIT 40000000
ALLOW FILTERING;
"""
start_time = timeit.default_timer()
print session.execute(query, timeout=None)[0]
print str((timeit.default_timer() - start_time)/60), " minutes elapsed for query 2\n"
#======================================================================
# QUERY 3
#======================================================================
query = """
SELECT count(*) as range_DUP_SEQ_NUM
FROM cdr
WHERE 
(CITY_ID,SERVICE_NODE_ID,RUM_DATA_NUM,MONTH_DAY,DUP_SEQ_NUM)
>(0,0,0,0,3000) AND
(CITY_ID,SERVICE_NODE_ID,RUM_DATA_NUM,MONTH_DAY,DUP_SEQ_NUM)
<(9900,9900,9900,9900,30000)
LIMIT 40000000
ALLOW FILTERING;
"""
start_time = timeit.default_timer()
temp = session.execute(query, timeout=None)[0]
print temp

print str((timeit.default_timer() - start_time) /60), " minutes elapsed for query 3\n"
#======================================================================
# QUERY 3 optimized
#======================================================================
query = """
SELECT count (*) as optimized_range_DUP_SEQ_NUM
FROM query3
WHERE (DUP_SEQ_NUM) > (3000) AND (DUP_SEQ_NUM) < (30000)
LIMIT 40000000
ALLOW FILTERING;
"""
start_time = timeit.default_timer()
temp = session.execute(query, timeout=None)[0]
print temp

print str((timeit.default_timer() - start_time) /60), " minutes elapsed for optimized query 3\n"
#======================================================================
# QUERY 4
#======================================================================
query = """
SELECT MOBILE_ID_TYPE,count
from GROUP_BY_MOBILE_ID_TYPE
"""
start_time = timeit.default_timer()
for row in session.execute(query, timeout=None):
    print row
print str((timeit.default_timer() - start_time)) + " seconds elapsed for query 4 table\n"

if loop:
    start_time = timeit.default_timer()
    for i in range (8):
        print session.execute(session.prepare("select count(*) from cdr where MOBILE_ID_TYPE = ?").bind([i]),timeout=None)
    print str((timeit.default_timer() - start_time)) + " seconds elapsed for query 4 loop\n"
#======================================================================
# QUERY 5
#======================================================================
query = """
SELECT month_day, count FROM group_by_month
"""
start_time = timeit.default_timer()
for row in (session.execute(query,timeout=None)):
    print row
print str((timeit.default_timer() - start_time))+ " seoncds elapsed for query 5 table\n"

if loop:
    start_time = timeit.default_timer()
    for i in range (1,31):
        print session.execute(session.prepare("select count(*) from cdr where month_day = ?").bind([i]),timeout=None)
    print str((timeit.default_timer() - start_time))+ " seoncds elapsed for query 5 loop\n"

print str((timeit.default_timer() - program_st)/60)+ " minutes elapsed for program\n"
