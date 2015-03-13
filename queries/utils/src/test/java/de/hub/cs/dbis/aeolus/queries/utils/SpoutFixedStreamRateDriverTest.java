package de.hub.cs.dbis.aeolus.queries.utils;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import de.hub.cs.dbis.aeolus.testUtils.IncSpout;





/**
 * @author Matthias J. Sax
 */
@RunWith(PowerMockRunner.class)
public class SpoutFixedStreamRateDriverTest {
	
	@Test
	public void testForwardCalls() {
		IRichSpout worker = mock(IRichSpout.class);
		SpoutFixedStreamRateDriver driver = new SpoutFixedStreamRateDriver(worker, 10);
		
		Config cfg = mock(Config.class);
		TopologyContext c = mock(TopologyContext.class);
		SpoutOutputCollector col = mock(SpoutOutputCollector.class);
		
		driver.open(cfg, c, col);
		verify(worker).open(cfg, c, col);
		
		driver.close();
		verify(worker).close();
		
		driver.activate();
		verify(worker).activate();
		
		driver.deactivate();
		verify(worker).deactivate();
		
		driver.nextTuple();
		verify(worker).nextTuple();
		
		Object messageId = new Object();
		driver.ack(messageId);
		verify(worker).ack(messageId);
		
		driver.fail(messageId);
		verify(worker).fail(messageId);
		
		OutputFieldsDeclarer declarer = mock(OutputFieldsDeclarer.class);
		driver.declareOutputFields(declarer);
		verify(worker).declareOutputFields(declarer);
		
		Map<String, Object> config = worker.getComponentConfiguration();
		Assert.assertEquals(config, driver.getComponentConfiguration());
	}
	
	@Test
	public void testNextTuple() {
		SpoutFixedStreamRateDriver driver = new SpoutFixedStreamRateDriver(new IncSpout(), 10);
		
		Config cfg = mock(Config.class);
		TopologyContext c = mock(TopologyContext.class);
		SpoutOutputCollector col = mock(SpoutOutputCollector.class);
		driver.open(cfg, c, col);
		
		driver.activate();
		long start = System.nanoTime();
		for(int i = 0; i < 20; ++i) {
			driver.nextTuple();
		}
		long stop = System.nanoTime();
		
		Assert.assertEquals(1900, (stop - start) / 1000 / 1000, -1);
	}
	
}
