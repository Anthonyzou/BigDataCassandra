import ast
import random
import time
from datetime import date
from xmlrpclib import MAXINT

import cassandra
from cassandra.cluster import Cluster


cluster = Cluster(['199.116.235.57', '10.0.0.31', '10.0.0.38', '127.0.0.1'])


session = cluster.connect('group3')  # keyspace should be our own
print cluster.metadata.cluster_name  # should make sure this is group3
print cassandra.__version__

# QUERY 1
start_time = time.clock()
query = ""
session.execute(query)
print str((time.clock() - start_time) / 60)[:7], "minutes elapsed"

# QUERY 2
start_time = time.clock()
query = ""
session.execute(query)
print str((time.clock() - start_time) / 60)[:7], "minutes elapsed"

# QUERY 3
start_time = time.clock()
query = ""
session.execute(query)
print str((time.clock() - start_time) / 60)[:7], "minutes elapsed"

# QUERY 4
start_time = time.clock()
query = ""
session.execute(query)
print str((time.clock() - start_time) / 60)[:7], "minutes elapsed"

# QUERY 5
start_time = time.clock()
query = ""
session.execute(query)
print str((time.clock() - start_time) / 60)[:7], "minutes elapsed"
