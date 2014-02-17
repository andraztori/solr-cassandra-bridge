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
	public static Logger log = LoggerFactory.getLogger(SimpleSolrTest.class);

	@BeforeClass
	public static void beforeClass() throws Exception {
		//initCore("solr/crazy-path-to-config.xml","solr/crazy-path-to-schema.xml");
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
