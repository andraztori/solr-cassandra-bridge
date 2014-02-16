import org.cassandraunit.CassandraUnit;
import org.cassandraunit.dataset.json.ClassPathJsonDataSet;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.cassandraunit.DataLoader;

class MyDataLoader extends DataLoader 
	{
		public MyDataLoader(String clusterName, String host) {
		            super(clusterName, host);
		}
		public void shutdown() {
			getCluster().getConnectionManager().shutdown();
		}
	}
