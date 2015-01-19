package storm.lrb.bolt;

import backtype.storm.Config;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import org.apache.log4j.Logger;

import storm.lrb.model.Accident;
import storm.lrb.model.AccidentImmutable;
import storm.lrb.model.PosReport;
import storm.lrb.model.Time;
import storm.lrb.tools.StopWatch;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 
 * 
 * this bolt computes the toll of a car holds the total cnt of cars
 * 
 * @author trillian
 * 
 */
public class AccidentNotificationBolt extends BaseRichBolt {

	private static final long serialVersionUID = 5537727428628598519L;
	private static final Logger LOG = Logger
			.getLogger(AccidentNotificationBolt.class);
	
	/**
	 *  contains all accidents;
	 */
	private ConcurrentHashMap<String, AccidentImmutable> allAccidents;
	
	// 
	//(because the lrb only emits accidentalerts if a vehicle crosses a new segment)
	/**
	 * contains all vehicle id's and xsd of last posistion report
	 */
	private ConcurrentHashMap<Integer, String> allCars;
	

	private OutputCollector collector;
	private int processed_xway = -1;

	
	StopWatch timer;
	

	public AccidentNotificationBolt() {
		allAccidents = new ConcurrentHashMap<String,AccidentImmutable>();
		allCars = new ConcurrentHashMap<Integer,String>();
		
		//allAccidents = new HashMap<String, AccidentInfo>();
		//timer = new StopWatch();
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		//timer.start();
	}

	@SuppressWarnings("unchecked")
	@Override
	public void execute(Tuple tuple) {

		if (tuple.contains("PosReport")) {
			
			PosReport pos = (PosReport) tuple.getValueByField("PosReport");
			//LOG.debug("AccidentNotification: check: "+pos.getVidAsString()+" on # "+pos.getXsd());
			String prevXsd = allCars.put(pos.getVid(), pos.getXsd());
			
			AccidentImmutable accident = allAccidents.get(pos.getXsd());
			
			if(accident!=null)
				sendAccidentAllert(pos,prevXsd, accident);
				
		} else if (tuple.contains("accidentInfo")) {
			
			updateAccidents(tuple);
	
		}
		// System.out.println("tollnotification: id-"+id+ " lav" +tuple);
		collector.ack(tuple);

	}

	private void updateAccidents(Tuple tuple) {
		String xsd = tuple.getStringByField("xsd");
		AccidentImmutable info = (AccidentImmutable) tuple.getValueByField("accidentInfo");
		LOG.debug("ACCNOT: recieved accident info");
		
		if(info.isOver()){
			allAccidents.remove(xsd);
			LOG.debug("ACCNOT: removed accident: "+ info);
		}
		else{
			AccidentImmutable prev = allAccidents.put(xsd, info);
			if (prev != null)
				LOG.debug("ACCNOT accident (prev: "+prev+")");
			else {
				LOG.debug("ACCNOT: added new accident");
			}
		}
	}

	private void sendAccidentAllert(PosReport pos, String prevXsd, AccidentImmutable accident) {
		//AccidentInfo info = allAccidents.get(accseg);
		
		//only emit notification if vehicle is not involved in accident and accident is still active
		if(accident.getInvolvedCars().contains(pos.getVid())){
			LOG.debug("no notification, becasue vid is accident vehicle");
				return ;
		}
		if(!accident.active(Time.getMinute(pos.getTime()))){
			LOG.debug("no notification, becasue accident is not active anymore");
			//TODO evtl nochmal aufräumen
			return ;
		}
		//only emit notification if vehicle crosses new segment and lane is not exit lane
		if(prevXsd!=pos.getXsd() && pos.getLane()!=4){
			String notification = accident.getAccNotification(pos);
			if(!notification.isEmpty()){
				collector.emit(new Values(notification));
			}
		}else
			LOG.debug("no acc notification because vehicle exits or was previously informed");
	}

	
	
	@Override
	public void cleanup() {
	/*	timer.stop();
		System.out.println("TollNotificationBolt was running for "+timer.getElapsedTimeSecs()+" seconds.");
		LOG.debug("TollNotificationBolt was running for "+timer.getElapsedTimeSecs()+" seconds.");
		*/
	}


	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("accnotification"));
	}

	/*
	 * @Override public Map<String, Object> getComponentConfiguration() {
	 * Map<String, Object> conf = new HashMap<String, Object>();
	 * //conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, updatefreq); return
	 * conf; }
	 */
}
