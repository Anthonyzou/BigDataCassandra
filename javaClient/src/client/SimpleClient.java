package client;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class SimpleClient {
	private Cluster cluster;

	public Session connect(String node) {
		cluster = Cluster.builder().addContactPoint(node).build();
		return cluster.connect("system");	
	}

	public void close() {
		cluster.shutdown();
	}

	public static void main(String[] args) {
		SimpleClient client = new SimpleClient();
		Session s = client.connect("127.0.0.1");
		s.getCluster();
		ResultSet rs = s.execute("SELECT keyspace_name, columnfamily_name FROM schema_columnfamilies");
		for(Row r : rs){
			System.out.println(r);
		}
		client.close();
		
	}
}