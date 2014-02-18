package com.zemanta.pysandra;
import org.json.simple.JSONObject;
import org.junit.Test;

import static org.junit.Assert.*;

import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PysandraTest
{
	public static Logger log = LoggerFactory.getLogger(PysandraTest.class);

	@Test
	public void pysandraFullTest() throws Exception {
		PysandraUnitClient puc = new PysandraUnitClient();
		puc.start_process();

		String current = new java.io.File( "." ).getCanonicalPath();
		puc.load_data(current + "/target/test-classes/cassandra-schema.json", "json");
		puc.clean_data();
		puc.stop_process();
	}
	
	
	@Test
	public void check()
	{
		assert(true);
		assertEquals(true, true);
	}
}
