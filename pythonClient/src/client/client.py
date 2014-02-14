from cassandra.cluster import Cluster

cluster = Cluster(['127.0.0.1'])
session = cluster.connect('system') #keyspace

rows = session.execute('SELECT keyspace_name, columnfamily_name FROM schema_columnfamilies')

for row in rows:
    print row
    