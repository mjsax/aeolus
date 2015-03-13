package storm.lrb.bolt;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.lrb.TopologyControl;
import storm.lrb.model.AccidentImmutable;
import storm.lrb.model.PosReport;
import storm.lrb.model.Time;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;





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
	private static final Logger LOG = LoggerFactory.getLogger(AccidentNotificationBolt.class);
	
	/**
	 * contains all accidents;
	 */
	private final Map<SegmentIdentifier, AccidentImmutable> allAccidents;
	
	//
	// (because the lrb only emits accidentalerts if a vehicle crosses a new segment)
	/**
	 * contains all vehicle id's and xsd of last posistion report
	 */
	private final Map<Integer, SegmentIdentifier> allCars;
	
	private OutputCollector collector;
	
	public AccidentNotificationBolt() {
		this.allAccidents = new HashMap<SegmentIdentifier, AccidentImmutable>();
		this.allCars = new HashMap<Integer, SegmentIdentifier>();
	}
	
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public void execute(Tuple tuple) {
		
		if(tuple.contains("PosReport")) {
			
			PosReport pos = (PosReport)tuple.getValueByField(TopologyControl.POS_REPORT_FIELD_NAME);
			// LOG.debug("AccidentNotification: check: "+pos.getVidAsString()+" on # "+pos.getXsd());
			SegmentIdentifier eventKey = new SegmentIdentifier(pos.getSegmentIdentifier().getxWay(), pos
				.getSegmentIdentifier().getSegment(), pos.getSegmentIdentifier().getDirection());
			SegmentIdentifier prevXsd = this.allCars.put(pos.getVehicleIdentifier(), eventKey);
			
			AccidentImmutable accident = this.allAccidents.get(eventKey);
			
			if(accident != null) {
				this.sendAccidentAllert(pos, eventKey, accident);
			}
			
		} else if(tuple.contains(TopologyControl.ACCIDENT_INFO_FIELD_NAME)) {
			
			this.updateAccidents(tuple);
			
		}
		LOG.debug("tollnotification: lav tuple %s", tuple);
		this.collector.ack(tuple);
		
	}
	
	private void updateAccidents(Tuple tuple) {
		PosReport pos = (PosReport)tuple.getValueByField(TopologyControl.POS_REPORT_FIELD_NAME);
		SegmentIdentifier accidentIdentifier = new SegmentIdentifier(pos.getSegmentIdentifier().getxWay(), pos
			.getSegmentIdentifier().getSegment(), pos.getSegmentIdentifier().getDirection());
		AccidentImmutable info = (AccidentImmutable)tuple.getValueByField(TopologyControl.ACCIDENT_INFO_FIELD_NAME);
		LOG.debug("recieved accident info '%s'", info);
		
		if(info.isOver()) {
			this.allAccidents.remove(accidentIdentifier);
			LOG.debug("removed accident '%s'", info);
		} else {
			AccidentImmutable prev = this.allAccidents.put(accidentIdentifier, info);
			if(prev != null) {
				LOG.debug("accident (prev accident: %s)", prev);
			} else {
				LOG.debug("added new accident");
			}
		}
	}
	
	private void sendAccidentAllert(PosReport pos, SegmentIdentifier prevXsd, AccidentImmutable accident) {
		// AccidentInfo info = allAccidents.get(accseg);
		
		// only emit notification if vehicle is not involved in accident and accident is still active
		if(accident.getInvolvedCars().contains(pos.getVehicleIdentifier())) {
			LOG.debug("no notification, becasue vid is accident vehicle");
			return;
		}
		if(!accident.active(Time.getMinute(pos.getTime()))) {
			LOG.debug("no notification, becasue accident is not active anymore");
			// TODO evtl nochmal aufräumen
			return;
		}
		// only emit notification if vehicle crosses new segment and lane is not exit lane
		if(!prevXsd.equals(prevXsd) && pos.getLane() != 4) {
			String notification = accident.getAccNotification(pos);
			if(!notification.isEmpty()) {
				this.collector.emit(new Values(notification));
			}
		} else {
			LOG.debug("no acc notification because vehicle exits or was previously informed");
		}
	}
	
	@Override
	public void cleanup() {}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(TopologyControl.ACCIDENT_NOTIFICATION_FIELD_NAME));
	}
}
