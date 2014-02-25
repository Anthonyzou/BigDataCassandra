from cassandra.cluster import Cluster

cluster = Cluster( ['10.0.0.31',
                    '10.0.0.38',
                    '127.0.0.1'
                    ])
session = cluster.connect('system') #keyspace should be our own


# query = "INSERT INTO users (id, name, age) VALUES (?, ?, ?)"
# prepared = session.prepare(query)
# session.execute(prepared, (user.id, user.name, user.age))
# 
# prepared = session.prepare(query)
# for user in users:
#      bound = prepared.bind((user.id, user.name, user.age))
#      session.execute(bound)
#      
# prepared = session.prepare(query)
# bound_stmt = prepared.bind((user.id, user.name, user.age))
# session.execute(bound_stmt)

rows = session.execute('SELECT keyspace_name, columnfamily_name FROM schema_columnfamilies')
for row in rows:
    print row
    