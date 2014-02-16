import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.solr.common.params.CommonParams;
import org.apache.solr.request.*;
import org.apache.solr.util.*;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.BeforeClass;


public class SimpleSolrTest extends SolrTestCaseJ4
{
	@Test
	public void check()
	{
		int a = 1;
		assert(true);
		assertEquals(true, true);
	}
}
