package de.hub.cs.dbis.aeolus.batching;

import java.util.ArrayList;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImpl;





/**
 * TODO
 * 
 * @author Matthias J. Sax
 */
public class InputDebatcher implements IRichBolt {
	private static final long serialVersionUID = 7781347435499103691L;
	
	private final static Logger LOGGER = LoggerFactory.getLogger(InputDebatcher.class);
	
	/**
	 * TODO
	 */
	private final IRichBolt wrappedBolt;
	/**
	 * TODO
	 */
	private TopologyContext topologyContext;
	
	
	/**
	 * TODO
	 * 
	 * @param bolt
	 */
	InputDebatcher(IRichBolt bolt) {
		this.wrappedBolt = bolt;
	}
	
	
	
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		this.topologyContext = context;
		this.wrappedBolt.prepare(stormConf, context, collector);
	}
	
	@Override
	public void execute(Tuple input) {
		LOGGER.trace("input: {}", input);
		
		if(input.getValues().getClass().getName().equals(Batch.class.getName())) {
			LOGGER.trace("debatching");
			
			final int numberOfAttributes = input.size();
			LOGGER.trace("numberOfAttributes: {}", numberOfAttributes);
			final BatchColumn[] columns = new BatchColumn[numberOfAttributes];
			
			for(int i = 0; i < numberOfAttributes; ++i) {
				columns[i] = (BatchColumn)input.getValue(i);
			}
			
			final int size = columns[0].size();
			LOGGER.trace("batchSize: {}", size);
			for(int i = 0; i < size; ++i) {
				final ArrayList<Object> attributes = new ArrayList<Object>(numberOfAttributes);
				
				for(int j = 0; j < numberOfAttributes; ++j) {
					attributes.add(columns[j].get(i));
				}
				LOGGER.trace("extracted tuple #{}: {}", i, attributes);
				
				final TupleImpl tuple = new TupleImpl(this.topologyContext, attributes, input.getSourceTask(),
					input.getSourceStreamId());
				
				this.wrappedBolt.execute(tuple);
			}
		} else {
			this.wrappedBolt.execute(input);
		}
		
	}
	
	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
	
}
