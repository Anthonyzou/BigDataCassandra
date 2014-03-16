import cassandra
from cassandra.cluster import Cluster
from datetime import date
import time

cluster = Cluster(['199.116.235.57', '10.0.0.31', '10.0.0.38', '127.0.0.1'])

session = cluster.connect('group3')  # keyspace should be our own
print cluster.metadata.cluster_name  # should make sure this is group3
print cassandra.__version__

#======================================================================
# QUERY 1
#======================================================================
start_time = time.clock()
query = """
SELECT count (*) 
FROM query1_2_3 
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
FROM query1_2_3
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
FROM query1_2_3
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
query = """
SELECT count (*) 
FROM query4 
ORDER BY 
        """
session.execute(query)
print str((time.clock() - start_time) / 60)[:7], "minutes elapsed"

#======================================================================
# QUERY 5
#======================================================================
start_time = time.clock()
query = """
SELECT count (*) 
FROM query5 
ORDER BY  
        """
    
session.execute(query)
print str((time.clock() - start_time) / 60)[:7], "minutes elapsed"

