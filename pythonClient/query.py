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
            CITY_ID                       > 100 AND
            MSC_CODE                      > 100 AND
            SESS_REQ_TYPE                 > 100 AND
            SESS_SFC                      > 100 AND
            SESS_OR_CONN_CPFAIL           > 100 AND
            SESS_SET_OR_TRA_REQ_TIME      > 100 AND
            SESS_SET_OR_TRA_COMP_TIME     > 100 AND
            CONN_REQ_TIME                 > 100 AND
            CONN_EST_TIME                 > 100 AND
            A12_RAN_AUTH_TIME             > 100 AND
            A10_LINK_EST_TIME             > 100 AND
            CONN_DUR                      > 100 AND
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
            CITY_ID          > 100 AND
            CITY_ID          < 1000
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
            MSC_CODE           > 100 AND
            MSC_CODE          < 1000
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
            SEQ_NUM           > 100 AND
            SEQ_NUM           < 1000
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
            CITY_ID           > 100 AND
            MSC_CODE          > 100 
        """
    
session.execute(query)
print str((time.clock() - start_time) / 60)[:7], "minutes elapsed"

#======================================================================
# QUERY 6
#======================================================================
start_time = time.clock()
query = """
            SELECT count (*) 
            FROM query6 
            WHERE 
            CITY_ID           > 100 AND
            MSC_CODE          > 100 
        """
    
session.execute(query)
print str((time.clock() - start_time) / 60)[:7], "minutes elapsed"
