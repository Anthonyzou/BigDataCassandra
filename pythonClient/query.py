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
            SELECT count (SEQ_NUM) 
            FROM query1_2 
            WHERE 
            CITY_ID                 > 100 
            MSC_CODE                > 100
            STARTTIME               > 100
            CONNEC_REQUEST_TIME     > 100 
            REPORT_TIME             > 100
            SEIZ_CELL_NUM_L         > 100
            SEIZ_SEC_NUM_L          > 100
            LAST_DRC_CELL_L         > 100
            LAST_DRC_SEC_L          > 100
            ASS_CELL_ID_FOR_CONN_L  > 100
        """
session.execute(query)
print str((time.clock() - start_time) / 60)[:7], "minutes elapsed"

#======================================================================
# QUERY 2
#======================================================================
start_time = time.clock()
query = """ 
            SELECT count (*) 
            FROM query1_2 
            WHERE
            CITY_ID          > 100 
            CITY_id          > 100
        """       
session.execute(query)
print str((time.clock() - start_time) / 60)[:7], "minutes elapsed"

#======================================================================
# QUERY 3
#======================================================================
start_time = time.clock()
query = """
            SELECT count (*) 
            FROM query3 
            WHERE 
            CITY_ID           > 100 
            MSC_CODE          > 100
            STARTTIME         > 100
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
            WHERE 
            CITY_ID           > 100 
            MSC_CODE          > 100
            STARTTIME         > 100
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
            WHERE 
            CITY_ID           > 100 
            MSC_CODE          > 100
            STARTTIME         > 100
        """
    
session.execute(query)
print str((time.clock() - start_time) / 60)[:7], "minutes elapsed"
