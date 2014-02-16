import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.BeforeClass;
import org.junit.AfterClass;

import pysandra.PysandraUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PysandraTest
{
	public static Logger log = LoggerFactory.getLogger(PysandraTest.class);

	static Process process;
	@BeforeClass
	public static void startPysandra() throws Exception {
		String separator = System.getProperty("file.separator");
		String classpath = System.getProperty("java.class.path");
		String path = System.getProperty("java.home")
			+ separator + "bin" + separator + "java";
		ProcessBuilder processBuilder = 
			new ProcessBuilder(path, "-cp", 
			classpath, 
			PysandraUnit.class.getName());
		process = processBuilder.start();
	}
	
	@Test
	public void check()
	{
		int a = 1;
		assert(true);
		assertEquals(true, true);
	}
}
