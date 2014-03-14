import ast
import random
import time
from datetime import date
from xmlrpclib import MAXINT

import cassandra
from cassandra.cluster import Cluster



start_time = time.clock()

cluster = Cluster(['199.116.235.57', '10.0.0.31', '10.0.0.38', '127.0.0.1'])


session = cluster.connect('group3')  # keyspace should be our own
print cluster.metadata.cluster_name  # should make sure this is group3
print cassandra.__version__

# QUERY 1

query = ""
session.execute(query)

# QUERY 2

query = ""
session.execute(query)

# QUERY 3

query = ""
session.execute(query)

# QUERY 4

query = ""
session.execute(query)

# QUERY 5

query = ""
session.execute(query)

