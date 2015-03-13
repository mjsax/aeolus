package storm.lrb.bolt;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.lrb.TopologyControl;
import storm.lrb.model.AccBalRequest;
import storm.lrb.model.DaiExpRequest;
import storm.lrb.model.PosReport;
import storm.lrb.model.TTEstRequest;
import storm.lrb.tools.StopWatch;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;





/**
 * 
 * This Bolt reduces the workload of the spout by taking over Tuple generation and disptching to the appropiate stream
 * as opposed to the disptacher bolt. emits positionreports per xway.
 * 
 */
public class DispatcherSplitBolt extends BaseRichBolt {
	
	/**
     *
     */
	private static final long serialVersionUID = 1L;
	
	private static final Logger LOG = LoggerFactory.getLogger(DispatcherSplitBolt.class);
	
	private int tupleCnt = 0;
	private OutputCollector collector;
	private StopWatch timer = null;
	private volatile boolean firstrun = true;
	
	public DispatcherSplitBolt() {
		this.timer = new StopWatch();
	}
	
	// TODO evtl buffered wschreiben
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext topologyContext, OutputCollector outputCollector) {
		this.collector = outputCollector;
		
	}
	
	@Override
	public void execute(Tuple tuple) {
		
		this.splitAndEmit(tuple);
		
		this.collector.ack(tuple);
	}
	
	private void splitAndEmit(Tuple tuple) {
		
		String line = tuple.getStringByField(TopologyControl.TUPLE_FIELD_NAME);
		if(this.firstrun) {
			this.firstrun = false;
			this.timer = (StopWatch)tuple.getValueByField(TopologyControl.TIMER_FIELD_NAME);
			LOG.info("Set timer: " + this.timer);
		}
		String tmp = line.substring(0, 1);
		if(!tmp.matches("^[0-4]")) {
			return;
		}
		
		try {
			
			switch(Integer.parseInt(tmp)) {
			case 0:
				PosReport pos = new PosReport(line, this.timer);
				
				if(this.tupleCnt <= 10) {
					LOG.debug(String.format("Created: %s", pos));
				}
				this.collector.emit(TopologyControl.POS_REPORTS_STREAM_ID, tuple, pos);
				this.tupleCnt++;
				break;
			case 2:
				AccBalRequest acc = new AccBalRequest(line, this.timer);
				this.collector.emit(TopologyControl.ACCOUNT_BALANCE_REQUESTS_STREAM_ID, tuple,
					new Values(acc.getVehicleIdentifier(), acc));
				break;
			case 3:
				DaiExpRequest exp = new DaiExpRequest(line, this.timer);
				this.collector.emit(TopologyControl.DAILY_EXPEDITURE_REQUESTS_STREAM_ID, tuple,
					new Values(exp.getVehicleIdentifier(), exp));
				break;
			case 4:
				TTEstRequest est = new TTEstRequest(line, this.timer);
				this.collector.emit(TopologyControl.TRAVEL_TIME_REQUEST_STREAM_ID, tuple,
					new Values(est.getVehicleIdentifier(), est));
				break;
			default:
				LOG.debug("Tupel does not match required LRB format" + line);
				
			}
			
		} catch(NumberFormatException e) {
			LOG.error(String.format("Error in line '%s'", line));
			throw new RuntimeException(e);
		} catch(IllegalArgumentException e) {
			LOG.error(String.format("Error in line '%s'", line));
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		
		outputFieldsDeclarer.declareStream(TopologyControl.POS_REPORTS_STREAM_ID, new Fields(
			TopologyControl.XWAY_FIELD_NAME, TopologyControl.SEGMENT_FIELD_NAME, TopologyControl.DIRECTION_FIELD_NAME,
			TopologyControl.VEHICLE_ID_FIELD_NAME, TopologyControl.POS_REPORT_FIELD_NAME));
		
		outputFieldsDeclarer.declareStream(TopologyControl.ACCOUNT_BALANCE_REQUESTS_STREAM_ID, new Fields(
			TopologyControl.VEHICLE_ID_FIELD_NAME, TopologyControl.ACCOUNT_BALANCE_REQUEST_FIELD_NAME));
		outputFieldsDeclarer.declareStream(TopologyControl.DAILY_EXPEDITURE_REQUESTS_STREAM_ID, new Fields(
			TopologyControl.VEHICLE_ID_FIELD_NAME, TopologyControl.DAILY_EXPEDITURE_REQUEST_FIELD_NAME));
		outputFieldsDeclarer.declareStream(TopologyControl.TRAVEL_TIME_REQUEST_STREAM_ID, new Fields(
			TopologyControl.VEHICLE_ID_FIELD_NAME, TopologyControl.TRAVEL_TIME_REQUEST_FIELD_NAME));
	}
	
	@Override
	public void cleanup() {
		super.cleanup();
	}
}
