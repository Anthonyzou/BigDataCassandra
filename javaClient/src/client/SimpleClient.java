package client;

import java.net.InetAddress;
import java.net.UnknownHostException;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class SimpleClient {
	private Cluster cluster;

	public Session connect(String ... node) {
		cluster = Cluster.builder()
						.addContactPoints(node)
						.withPort(9042)
						.build();
		return cluster.connect("system");	
	}

	public void close() {
		cluster.shutdown();
	}

	public static void main(String[] args) throws UnknownHostException {
		SimpleClient client = new SimpleClient();
		Session s = client.connect( "prereading.dlinkddns.com", 
									"10.0.0.31", 
									"10.0.0.38");
		s.getCluster();
		ResultSet rs = s.execute("SELECT keyspace_name, columnfamily_name FROM schema_columnfamilies");
		for(Row r : rs){
			System.out.println(r);
		}
		client.close();
		
	}
}