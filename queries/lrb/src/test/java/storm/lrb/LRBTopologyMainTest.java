package storm.lrb;

import java.io.File;
import java.util.Arrays;
import java.util.LinkedList;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;





/**
 * 
 * @author richter
 */
public class LRBTopologyMainTest {
	
	@BeforeClass
	public static void setUpClass() {}
	
	@AfterClass
	public static void tearDownClass() {}
	
	public LRBTopologyMainTest() {}
	
	@Before
	public void setUp() {}
	
	@After
	public void tearDown() {}
	
	/**
	 * Test of main method, of class LRBTopologyMain.
	 * 
	 * @throws java.lang.Exception
	 */
	@Test
	public void testMain0() throws Exception {
		String histFilePath = File.createTempFile("lrb-test", null).getAbsolutePath();
		LRBTopologyMain.main0(
			0, // offset
			1, // executors
			2, // xways
			"127.0.0.1", // host
			5060, // port
			histFilePath,
			2, // tasks,
			new LinkedList<String>(Arrays
				.asList(TopologyControl.XWAY_FIELD_NAME, TopologyControl.VEHICLE_ID_FIELD_NAME)), // fields
			false, // submit
			true, // stormConfigDebug
			2, // workers
			"nameext", // nameext
			5000 // runtimeMillis
			);
	}
	
}
