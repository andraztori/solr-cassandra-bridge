import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.solr.common.params.CommonParams;
import org.apache.solr.request.*;
import org.apache.solr.util.*;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.BeforeClass;


public class SimpleSolrTest extends SolrTestCaseJ4
{
	@BeforeClass
	public static void beforeClass() throws Exception {
		//initCore("solr/crazy-path-to-config.xml","solr/crazy-path-to-schema.xml");
		initCore("solrconfig.xml","schema.xml");
		
	}
          

	@Test
	public void check()
	{
		int a = 1;
		assert(true);
		assertEquals(true, true);
	}
}
