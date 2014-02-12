class Client
  require 'cql'
  
  client = Cql::Client.connect(hosts: ['127.0.0.1'])
  client.use('system')
#  rows = client.execute('SELECT * FROM call_details_record')
#  rows.each do |row|
#    puts row
#  end
  
  rows = client.execute('SELECT keyspace_name, columnfamily_name FROM schema_columnfamilies')
  rows.each do |row|
    puts "The keyspace #{row['keyspace_name']} has a table called #{row['columnfamily_name']}"
  end
end