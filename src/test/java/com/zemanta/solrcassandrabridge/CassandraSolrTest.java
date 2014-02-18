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
		puc.clean_data();
		puc.load_data(current + "/target/test-classes/cassandra-schema.json", "json");
		log.info("Pysandra running, starting solr");
		initCore("solrconfig-bridge.xml","schema.xml");
		log.info("Solr core running");

		// Now load the articles into solr 
		// These two articles have fields "body" and "title" availabe in cassandra 
		assertU(adoc("id", "1001", 
				"title", "Article1 title - INDEXED, NOT STORED", 
				"body", "Article1 body - INDEXED, NOT STORED",
				"url", "http://www.article1.com/"
				));
	
		assertU(adoc("id", "1002", 
				"title", "Article2 title - INDEXED, NOT STORED", 
				"body", "Article2 body - INDEXED, NOT STORED",
				"url", "http://www.article2.com/"
				));

		// This one has key, but no fields in cassandra
		assertU(adoc("id", "1003", 
				"title", "Article3 title - INDEXED, NOT STORED", 
				"body", "Article3 body - INDEXED, NOT STORED",
				"url", "http://www.article3.com/"
				));

		// This one has no key in cassandra
		assertU(adoc("id", "1004", 
				"title", "Article4 title - INDEXED, NOT STORED", 
				"body", "Article4 body - INDEXED, NOT STORED",
				"url", "http://www.article4.com/"
				));
	    assertU(commit());

	}

	@AfterClass
	public static void after() throws Exception {	
		log.info("Shutting down pysandrat");  
		puc.stop_process();
	}
          

	@Test
	public void dummyTest()
	{
		assert(true);
	}
 
	@Test
	public void docTest()
	{
		// Only one article matches!
		SolrQueryRequest req;
		req = lrf.makeRequest("q", "title:\"article1\"" );
		assertQ("exaclty one article should be found",
					req,
					"//result[@numFound=1]",
					"//result/doc[1]/int[@name='id'][. ='1001']"
				);

		
		req = lrf.makeRequest("q", "title:\"article1\"", "fl", "id,body,title");
		assertQ("exaclty one article should be found",
				req,
				"//result/doc[1]/str[@name='title'][. ='Article1 Title1']"
			);
			
	}
}

