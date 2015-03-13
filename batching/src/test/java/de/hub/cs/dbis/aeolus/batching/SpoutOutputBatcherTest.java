package de.hub.cs.dbis.aeolus.batching;

import static org.mockito.Mockito.mock;

import java.util.Random;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import backtype.storm.topology.IRichSpout;





/**
 * @author Matthias J. Sax
 */
public class SpoutOutputBatcherTest {
	private static IRichSpout spoutMockStatic;
	
	private long seed;
	private Random r;
	
	@BeforeClass
	public static void prepareStatic() {
		spoutMockStatic = mock(IRichSpout.class);
	}
	
	@Before
	public void prepare() {
		this.seed = System.currentTimeMillis();
		this.r = new Random(this.seed);
		System.out.println("Test seed: " + this.seed);
		
	}
	
	@Test
	public void testOpen() {
		SpoutOutputBatcher spout = new SpoutOutputBatcher(spoutMockStatic, 10);
		
		// spout.open(null, null, null);
	}
	
	@Test
	public void testClose() {
		
	}
	
	@Test
	public void testActivate() {
		
	}
	
	@Test
	public void testDeactivate() {
		
	}
	
	@Test
	public void testAck() {
		
	}
	
	@Test
	public void testFail() {
		
	}
	
	@Test
	public void testDeclareOutputFields() {
		
	}
	
	@Test
	public void testGetComponentConfiguration() {}
	
}
