package de.hub.cs.dbis.aeolus.testUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;





/**
 * {@link ForwardBolt} forwards each incoming tuple to one or multiple output streams (ie, data is replicated to all
 * output streams). All incoming tuples must have the same schema. {@link ForwardBolt} acknowledges all forwarded
 * tuples.
 * 
 * @author Matthias J. Sax
 */
// TODO add .emitDirect(...) support
public class ForwardBolt implements IRichBolt {
	private static final long serialVersionUID = -2047329782139913124L;
	private final static Logger logger = LoggerFactory.getLogger(ForwardBolt.class);
	
	private final Fields tupleSchema;
	private final String[] outputStreams;
	private OutputCollector collector;
	
	
	
	/**
	 * Instantiates a new {@link ForwardBolt} for the given tuple schema that emits all tuples to the default output
	 * stream.
	 * 
	 * @param schema
	 *            The schema of the input (and output) tuples.
	 */
	public ForwardBolt(Fields schema) {
		this(schema, new String[] {Utils.DEFAULT_STREAM_ID});
	}
	
	/**
	 * Instantiates a new {@link ForwardBolt} for the given tuple schema that emits all tuples to the given output
	 * streams.
	 * 
	 * @param schema
	 *            The schema of the input (and output) tuples.
	 * @param outputStreamIds
	 *            The IDs of the output stream to use.
	 */
	public ForwardBolt(Fields schema, String[] outputStreamIds) {
		assert (schema != null);
		assert (schema.size() > 0);
		assert (outputStreamIds != null);
		assert (outputStreamIds.length > 0);
		
		this.tupleSchema = schema;
		this.outputStreams = Arrays.copyOf(outputStreamIds, outputStreamIds.length);
	}
	
	
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, @SuppressWarnings("hiding") OutputCollector collector) {
		this.collector = collector;
	}
	
	@Override
	public void execute(Tuple input) {
		for(String streamId : this.outputStreams) {
			List<Integer> receiverIds = this.collector.emit(streamId, input.getValues());
			logger.trace("forwarded tuple {} to output stream {} to receiver tasks with IDs {}", input, streamId,
				receiverIds);
		}
		
		this.collector.ack(input);
	}
	
	@Override
	public void cleanup() {
		// nothing to do
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		for(String streamId : this.outputStreams) {
			declarer.declareStream(streamId, this.tupleSchema);
		}
	}
	
	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
	
}
