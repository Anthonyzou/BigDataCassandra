import cassandra
from cassandra.cluster import Cluster
from datetime import date
import time

cluster = Cluster(['199.116.235.57', '10.0.0.31', '10.0.0.38', '127.0.0.1'],port=9233)

session = cluster.connect('group3')  # keyspace should be our own
print cluster.metadata.cluster_name  # should make sure this is group3
print cassandra.__version__

#======================================================================
# QUERY 1
#======================================================================
start_time = time.clock()
query = """
SELECT count (*) 
FROM cdr 
WHERE
(CITY_ID,PCMD_VER  ,SEQ_NUM ,MONTH_DAY ,DUP_SEQ_NUM ,MOBILE_ID_TYPE ,SESS_REQ_TYPE ,SESS_SFC ,SESS_OR_CONN_CPFAIL ,CFC)
> (5000,5000,5000,5000,5000,5000,5000,5000,5000,5000) AND
(CITY_ID,PCMD_VER  ,SEQ_NUM ,MONTH_DAY ,DUP_SEQ_NUM ,MOBILE_ID_TYPE ,SESS_REQ_TYPE ,SESS_SFC ,SESS_OR_CONN_CPFAIL ,CFC)
< (70000,70000,70000,70000,70000,70000,70000,70000,70000,70000)
ALLOW FILTERING
        """
session.execute(query)
print str((time.clock() - start_time) / 60)[:7], "minutes elapsed"

#======================================================================
# QUERY 2
#======================================================================
start_time = time.clock()
query = """ 
SELECT count (*) 
FROM cdr
WHERE
(CITY_ID) > (5000) AND (CITY_ID ) < (70000)
ALLOW FILTERING
        """       
session.execute(query)
print str((time.clock() - start_time) / 60)[:7], "minutes elapsed"

#======================================================================
# QUERY 3
#======================================================================
start_time = time.clock()
query = """
SELECT count (*) 
FROM cdr
WHERE 
msc_code = 3 and (CITY_ID) > (5000) AND (CITY_ID ) < (70000)
ALLOW FILTERING
        """
session.execute(query)
print str((time.clock() - start_time) / 60)[:7], "minutes elapsed"

#======================================================================
# QUERY 4
#======================================================================
start_time = time.clock()

prepared = session.prepare("""
                SELECT count (*) 
                FROM cdr 
                where msc_code = ?
                ORDER BY 
                """)
for x in range(0,7):
    session.execute(prepared.bind([x]))
print str((time.clock() - start_time) / 60)[:7], "minutes elapsed"

#======================================================================
# QUERY 5
#======================================================================
start_time = time.clock()

prepared = session.prepare("""
                SELECT count (*) 
                FROM cdr 
                where msc_code = ?
                ORDER BY city_id
                """)
for x in range(0,7):
    session.execute(prepared.bind([x]))
    
session.execute(query)
print str((time.clock() - start_time) / 60)[:7], "minutes elapsed"

