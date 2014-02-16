from cassandra.cluster import Cluster

cluster = Cluster( ['10.0.0.31', 
                    '10.0.0.38', 
                    '127.0.0.1'])

session = cluster.connect('system') #keyspace

rows = session.execute('SELECT keyspace_name, columnfamily_name FROM schema_columnfamilies')

for row in rows:
    print row
    