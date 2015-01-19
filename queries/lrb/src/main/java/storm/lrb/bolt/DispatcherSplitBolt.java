package storm.lrb.bolt;


import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

import org.apache.log4j.Logger;

import storm.lrb.model.AccBalRequest;
import storm.lrb.model.DaiExpRequest;
import storm.lrb.model.PosReport;
import storm.lrb.model.TTEstRequest;
import storm.lrb.tools.StopWatch;

/**
 * 
 * This Bolt reduces the workload of the spout by taking over Tuple generation
 * and disptching to the appropiate stream as opposed to the disptacher bolt.
 * emits positionreports per xway.
 * 
 */
public class DispatcherSplitBolt extends BaseRichBolt {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private static final Logger LOG = Logger
			.getLogger(DispatcherSplitBolt.class);
	int tupleCnt = 0;
	private OutputCollector _collector;
	private StopWatch timer=null;
	private int offset = 0;
	private volatile boolean firstrun = true;
	private final int processed_xway;

	public DispatcherSplitBolt(int xways) {
		this.timer = new StopWatch();
		processed_xway = xways;
	}

	// TODO evtl buffered wschreiben
	@Override
	public void prepare(Map conf, TopologyContext topologyContext,
			OutputCollector outputCollector) {
		_collector = outputCollector;

	}

	@Override
	public void execute(Tuple tuple) {
		
		splitAndEmit(tuple);
		
		_collector.ack(tuple);
	}

	private void splitAndEmit(Tuple tuple) {
		
		
		String line = tuple.getString(0);
		if(firstrun){
			firstrun = false;
			timer = (StopWatch) tuple.getValue(1);
			LOG.info("Set timer: "+timer);
		}
		String tmp = line.substring(0,1);
		if(!tmp.matches("^[0-4]")) return;
	
		
		try {
			
			switch (Integer.parseInt(tmp)) {
			case 0:
				PosReport pos = new PosReport(line, timer);
				
				String output_stream = "PosReports_"+pos.getSegmentIdentifier().toString();
				if(tupleCnt<=10) {
					LOG.debug(String.format("Created: %s", pos));
				}
				_collector.emit(output_stream,tuple,pos);
				tupleCnt++;
				break;
			case 2:
				AccBalRequest acc = new AccBalRequest(line, timer);
				_collector.emit("AccBalRequests", tuple, 
						new Values(acc.getVehicleIdentifier(),acc));
				break;
			case 3:
				DaiExpRequest exp = new DaiExpRequest(line, timer);
				_collector.emit("DaiExpRequests", tuple,new Values(exp.getVehicleIdentifier(),  exp));
				break;
			case 4:
				TTEstRequest est = new TTEstRequest(line, timer);
				_collector.emit("TTEstRequests", tuple, new Values(est.getVehicleIdentifier(),est));
				break;
			default:
				LOG.debug("Tupel does not match required LRB format" + line);
				
			}
			
		} catch (NumberFormatException e) {
			e.printStackTrace();
			System.out.println("Fehler"+line);
		}catch(IllegalArgumentException e){
			System.out.println("Fehler"+line);
			e.printStackTrace();
		}
	}

	
	private void _checkOffset(String tuple) {
		
		String[] tupel;
		tupel = tuple.split(",");
       
		offset = Integer.parseInt(tupel[1]);
		System.out.println("DISPATCHER:: set offset"+timer.getElapsedTime());
		if(offset>0) timer.setOffset(offset);
		
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

		for ( int faktor = 0; faktor < processed_xway ; faktor ++ ) {
			outputFieldsDeclarer.declareStream("PosReports_"+faktor, new Fields("xway", "dir", "xd","xsd", "vid", "PosReport"));
		}
		
		// declarer.declareStream("PosReports", new Fields("xway", "xsd", "vid",
		// "PosReport"));

		outputFieldsDeclarer.declareStream("AccBalRequests", new Fields("vid","AccBalRequests"));
		outputFieldsDeclarer.declareStream("DaiExpRequests", new Fields("vid","DaiExpRequests"));
		outputFieldsDeclarer.declareStream("TTEstRequests", new Fields("vid","TTEstRequests"));
	}

	

	@Override
	public void cleanup() {
		
		super.cleanup();

	}
}