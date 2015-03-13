package de.hub.cs.dbis.aeolus.testUtils;

import static org.mockito.Mockito.mock;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;

import backtype.storm.tuple.Fields;





/**
 * @author Matthias J. Sax
 */
@RunWith(PowerMockRunner.class)
public class TestDeclarerTest {
	
	@Test
	public void testDeclare() {
		TestDeclarer declarer = new TestDeclarer();
		
		Fields[] schema = new Fields[] {mock(Fields.class), mock(Fields.class), mock(Fields.class), mock(Fields.class)};
		String[] stream = new String[] {"stream1", "stream2"};
		
		declarer.declare(schema[0]);
		declarer.declare(true, schema[1]);
		declarer.declareStream(stream[0], schema[2]);
		declarer.declareStream(stream[1], true, schema[3]);
		
		Assert.assertEquals(4, declarer.schema.size());
		Assert.assertEquals(4, declarer.streamId.size());
		Assert.assertEquals(4, declarer.direct.size());
		
		Assert.assertSame(schema[0], declarer.schema.get(0));
		Assert.assertEquals(null, declarer.streamId.get(0));
		Assert.assertEquals(false, declarer.direct.get(0));
		
		Assert.assertSame(schema[1], declarer.schema.get(1));
		Assert.assertEquals(null, declarer.streamId.get(1));
		Assert.assertEquals(true, declarer.direct.get(1));
		
		Assert.assertSame(schema[2], declarer.schema.get(2));
		Assert.assertEquals(stream[0], declarer.streamId.get(2));
		Assert.assertEquals(false, declarer.direct.get(2));
		
		Assert.assertSame(schema[3], declarer.schema.get(3));
		Assert.assertEquals(stream[1], declarer.streamId.get(3));
		Assert.assertEquals(true, declarer.direct.get(3));
	}
}
