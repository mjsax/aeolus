package de.hub.cs.dbis.aeolus.testUtils;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;





/**
 * {@link ResultBolt} accumulates all received tuples and emits no tuples. {@link ResultBolt} acknowledges all received
 * tuples.
 * 
 * @author Matthias J. Sax
 */
public class ResultBolt implements IRichBolt {
	private static final long serialVersionUID = 7985730498618052164L;
	
	private final static Logger LOGGER = LoggerFactory.getLogger(ResultBolt.class);
	
	
	
	private OutputCollector collector;
	public LinkedList<List<Object>> receivedTuples = new LinkedList<List<Object>>();
	
	
	
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, @SuppressWarnings("hiding") OutputCollector collector) {
		this.collector = collector;
	}
	
	@Override
	public void execute(Tuple input) {
		LOGGER.trace("Buffering result tuple: {}", input);
		this.receivedTuples.add(input.getValues());
		this.collector.ack(input);
	}
	
	@Override
	public void cleanup() {
		// nothing to do
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// nothing to do
	}
	
	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
	
}
