package com.zemanta.solrcassandrabridge;
import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.solr.common.params.CommonParams;
import org.apache.solr.request.*;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.BeforeClass;
import org.junit.BeforeClass;
import org.junit.AfterClass;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleSolrTest extends SolrTestCaseJ4
{
	/*  
	 * The only function of this test is independently verify environment set-up for solr testing framework
	 */
	public static Logger log = LoggerFactory.getLogger(SimpleSolrTest.class);

	@BeforeClass
	public static void beforeClass() throws Exception {
		initCore("solrconfig-plain.xml","schema.xml");
		
	}
          

	@Test
	public void check()
	{
		int a = 1;
		assert(true);
		assertEquals(true, true);
	}
}
