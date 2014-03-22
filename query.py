import cassandra
from cassandra.cluster import Cluster
import timeit
from _socket import timeout

cluster = Cluster(
    ['199.116.235.57', '10.0.0.31', '10.0.0.38', '127.0.0.1'], port=9233)

session = cluster.connect('group3')  # keyspace should be our own

print cluster.metadata.cluster_name  # should make sure this is group3
print cassandra.__version__
print ""
program_st = timeit.default_timer()
#======================================================================
# QUERY 1
#======================================================================
start_time = timeit.default_timer()
query = """
SELECT count (*) as ten_atomic
FROM cdr 
WHERE 
(CITY_ID,SERVICE_NODE_ID,RUM_DATA_NUM,MONTH_DAY,DUP_SEQ_NUM,MOBILE_ID_TYPE,SEIZ_CELL_NUM,FLOW_DATA_INC,SUB_HOME_INT_PRI,CON_OHM_NUM) 
> (50000,50000,50000,50000,50000,50000,50000,50000,50000,50000)
AND (CITY_ID,SERVICE_NODE_ID,RUM_DATA_NUM,MONTH_DAY,DUP_SEQ_NUM,MOBILE_ID_TYPE,SEIZ_CELL_NUM,FLOW_DATA_INC,SUB_HOME_INT_PRI,CON_OHM_NUM) 
< (70000,70000,70000,70000,70000,70000,70000,70000,70000,70000)
LIMIT 40000000
ALLOW FILTERING ;
"""
result = session.execute(query, timeout=None)[0]
print result

print str((timeit.default_timer() - start_time) ), "seconds elapsed for query 1\n"
#======================================================================
# QUERY 2
#======================================================================
start_time = timeit.default_timer()
query = """ 
SELECT count (*) as range_city_id
FROM cdr
WHERE
CITY_ID > 5000 AND CITY_ID < 70000
LIMIT 40000000
ALLOW FILTERING;
"""
print session.execute(query, timeout=None)[0]
print str((timeit.default_timer() - start_time)), "seconds elapsed for query 2\n"
#======================================================================
# QUERY 3
#======================================================================
start_time = timeit.default_timer()
query = """
SELECT count (*) as MOBILE_ID_TYPE
FROM cdr
WHERE 
(CITY_ID,SERVICE_NODE_ID,RUM_DATA_NUM,MONTH_DAY,DUP_SEQ_NUM,MOBILE_ID_TYPE)
>(0,0,0,0,0,30000) AND
(CITY_ID,SERVICE_NODE_ID,RUM_DATA_NUM,MONTH_DAY,DUP_SEQ_NUM,MOBILE_ID_TYPE)
<(4000000,4000000,4000000,4000000,4000000,50000)
LIMIT 40000000
ALLOW FILTERING;
"""
temp = session.execute(query, timeout=None)[0]
print temp

print str((timeit.default_timer() - start_time) ), "seconds elapsed for query 3\n"
#======================================================================
# QUERY 4
#======================================================================
start_time = timeit.default_timer()

query = """
SELECT count (*) as group_by_msc_code_??
FROM cdr 
where msc_code = ?
ORDER BY city_id
LIMIT 40000000;
"""
for x in range(8):
    result = session.execute(session.prepare(query.replace("??", str(x))).bind([x]), timeout=None)
    print result
print str((timeit.default_timer() - start_time) ) + " seconds elapsed for query 4\n"
#======================================================================
# QUERY 5
#======================================================================
start_time = timeit.default_timer()

query = """
SELECT * FROM group_by_month
"""
rows =  (session.execute(query,timeout=None))
for row in rows:
    print row
    
print str((timeit.default_timer() - start_time) ), "seconds elapsed for query 5\n"
print str((timeit.default_timer() - program_st)), "seconds elapsed for program\n"
