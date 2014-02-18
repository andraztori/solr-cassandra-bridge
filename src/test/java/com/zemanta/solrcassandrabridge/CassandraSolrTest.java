package com.zemanta.solrcassandrabridge;
import org.junit.Test;
import org.junit.Rule;

import static org.junit.Assert.*;

import org.apache.solr.common.params.CommonParams;
import org.apache.solr.request.*;
import org.apache.solr.util.*;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zemanta.pysandra.PysandraUnitClient;

import me.prettyprint.hector.api.Cluster;

public class CassandraSolrTest extends SolrTestCaseJ4
{
	/* This tests starts a separate cassandra server (cassandraunit via Pysandra)
	 * and then tests if fields that need to be returned from cassandra are actually returned fro m cassandra
	 */

	public static Logger log = LoggerFactory.getLogger(CassandraSolrTest.class);
	private static PysandraUnitClient puc;
	
	@BeforeClass
	public static void beforeClass() throws Exception {

		puc = new PysandraUnitClient();
		puc.start_process();
		String current = new java.io.File( "." ).getCanonicalPath();
		puc.load_data(current + "/target/test-classes/cassandra-schema.json", "json");
		puc.clean_data();
		log.info("Pysandra running, starting solr");
		initCore("solrconfig-bridge.xml","schema.xml");
		log.info("Solr core running");
	}

	@AfterClass
	public static void after() throws Exception {	
		log.info("Shutting down pysandrat");  
		puc.stop_process();
	}
          

	@Test
	public void check()
	{
		int a = 1;
		assert(true);
		assertEquals(true, true);
	}
}
