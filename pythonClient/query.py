import cassandra
from cassandra.cluster import Cluster
import time

cluster = Cluster(['199.116.235.57', '10.0.0.31', '10.0.0.38', '127.0.0.1'],port=9233)

session = cluster.connect('group3')  # keyspace should be our own
print cluster.metadata.cluster_name  # should make sure this is group3
print cassandra.__version__
print ""
program_st = time.clock()
#======================================================================
# QUERY 1
#======================================================================
start_time = time.clock()
query = """
SELECT count (*) as ten_atomic
FROM cdr 
WHERE 
(CITY_ID,PCMD_VER,SEQ_NUM,MONTH_DAY,DUP_SEQ_NUM,MOBILE_ID_TYPE,SESS_REQ_TYPE,SESS_SFC,SESS_OR_CONN_CPFAIL,CFC) 
> (50000,50000,50000,50000,50000,50000,50000,50000,50000,50000)
AND (CITY_ID,PCMD_VER,SEQ_NUM,MONTH_DAY,DUP_SEQ_NUM,MOBILE_ID_TYPE,SESS_REQ_TYPE,SESS_SFC,SESS_OR_CONN_CPFAIL,CFC) 
< (70000,70000,70000,70000,70000,70000,70000,70000,70000,70000)
LIMIT 40000000
ALLOW FILTERING 
"""
print session.execute(query,timeout=None)[0]
print str((time.clock() - start_time) / 60)[:7], "minutes elapsed\n"

#======================================================================
# QUERY 2
#======================================================================
start_time = time.clock()
query = """ 
SELECT  FIRST 3 REVERSED 'CITY_ID5000'..'CITY_ID70000'
FROM cdr
LIMIT 40000000
ALLOW FILTERING
"""       
print session.execute(query,timeout=None)[0]
print str((time.clock() - start_time) / 60)[:7], "minutes elapsed\n"

#======================================================================
# QUERY 3
#======================================================================
start_time = time.clock()
query = """
SELECT count (*) as range_msc_code
FROM cdr
WHERE 
CITY_ID > 5000 AND CITY_ID < 70000
LIMIT 40000000
ALLOW FILTERING
"""
print session.execute(query,timeout=None)[0]
print str((time.clock() - start_time) / 60)[:7], "minutes elapsed\n"

#======================================================================
# QUERY 4
#======================================================================
start_time = time.clock()

query = """
SELECT count (*) as group_by_msc_code_??
FROM cdr 
where msc_code = ?
ORDER BY city_id
LIMIT 40000000
"""
future = []
for x in range(8):
    future.append(session.execute_async(session.prepare(query.replace("??", str(x))).bind([x])))
for query in future:
    print query.result()[0]
print str((time.clock() - start_time) / 60)[:7], "minutes elapsed\n"

#======================================================================
# QUERY 5
#======================================================================
start_time = time.clock()

query = """
SELECT count (*) as group_by_day_of_month_??
FROM cdr 
where month_day = ?
LIMIT 40000000
"""
future = []
for x in range(1,32):
    future.append(session.execute_async(session.prepare(query.replace("??", str(x))).bind([x])))   
for query in future:
    print (query.result()[0])
print str((time.clock() - start_time) / 60)[:7], "minutes elapsed\n"
print str((time.clock() - program_st) / 60)[:7], "minutes elapsed"
