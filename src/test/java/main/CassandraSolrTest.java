import org.junit.Test;
import org.junit.Rule;
import static org.junit.Assert.*;

import org.cassandraunit.CassandraUnit;
import org.cassandraunit.dataset.json.ClassPathJsonDataSet;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.cassandraunit.DataLoader;

import org.apache.solr.common.params.CommonParams;
import org.apache.solr.request.*;
import org.apache.solr.util.*;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import me.prettyprint.hector.api.Cluster;

public class CassandraSolrTest extends SolrTestCaseJ4
{

	public static Logger log = LoggerFactory.getLogger(CassandraSolrTest.class);
	static MyDataLoader dataLoader = new MyDataLoader("TestCluster", "localhost:9171");
		
	@BeforeClass
	public static void beforeClass() throws Exception {
		initCore("solrconfig-bridge.xml","schema.xml");

		EmbeddedCassandraServerHelper.startEmbeddedCassandra();
		dataLoader.load(new ClassPathJsonDataSet("cassandra-schema.json"));
		log.info("aAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");  
	}

	@AfterClass
	public static void after() throws Exception {	
		EmbeddedCassandraServerHelper.stopEmbeddedCassandra();
		dataLoader.shutdown();
		
		log.info("BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBb");  
	}
          

	@Test
	public void check()
	{
		int a = 1;
		assert(true);
		assertEquals(true, true);
	}
}
