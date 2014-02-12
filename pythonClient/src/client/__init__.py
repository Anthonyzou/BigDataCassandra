from cassandra.cluster import Cluster

cluster = Cluster(['127.0.0.1'])
session = cluster.connect('cmput391') #keyspace

rows = session.execute('SELECT * from call_details_record')
for row in rows:
    print row