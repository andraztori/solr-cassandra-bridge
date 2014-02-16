import org.junit.Test;
import org.junit.Rule;
import static org.junit.Assert.*;

import org.cassandraunit.CassandraUnit;
import org.cassandraunit.dataset.json.ClassPathJsonDataSet;


import org.apache.solr.common.params.CommonParams;
import org.apache.solr.request.*;
import org.apache.solr.util.*;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.BeforeClass;


public class CassandraSolrTest extends SolrTestCaseJ4
{

	
///	@Rule
//	public CassandraUnit cassandraUnit = new CassandraUnit(new ClassPathJsonDataSet("cassandra-schema.json"));
	
	
	@BeforeClass
	public static void beforeClass() throws Exception {
		//initCore("solr/crazy-path-to-config.xml","solr/crazy-path-to-schema.xml");
		initCore("solrconfig-bridge.xml","schema.xml");
		
	}
          

	@Test
	public void check()
	{
		int a = 1;
		assert(true);
		assertEquals(true, true);
	}
}
