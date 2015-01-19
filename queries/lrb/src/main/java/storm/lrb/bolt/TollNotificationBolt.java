package storm.lrb.bolt;


import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import org.apache.log4j.Logger;

import storm.lrb.model.AccidentImmutable;
import storm.lrb.model.NovLav;
import storm.lrb.model.Time;
import storm.lrb.model.VehicleInfo;
import storm.lrb.model.PosReport;
import storm.lrb.tools.StopWatch;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/**
 * This bolt calculates the actual toll for each vehicle depending on the
 * congestion and accident status of the segment the vehicle is driving on. It
 * can process streams containing position reports, accident information and 
 * novLavs. 
 */
public class TollNotificationBolt extends BaseRichBolt {

	private static final long serialVersionUID = 5537727428628598519L;
	private static final Logger LOG = Logger.getLogger(TollNotificationBolt.class);

	protected static final int MAX_SPEED_FOR_TOLL = 40;
	protected static final int MIN_CARS_FOR_TOLL = 50;
	protected static int DRIVE_EASY = 0;
	
	/**
	 * Holds all lavs (avgs of preceeding minute) and novs (number of vehicles) of the
	 * preceeding minute in a segment
	 * segment -> NovLav
	 */
	private final ConcurrentHashMap<SegmentIdentifier, NovLav> allNovLavs;

	/**
	 * holds vehicle information of all vehicles driving
	 * (vid -> vehicleinfo)
	 */
	private final ConcurrentHashMap<Integer, VehicleInfo> allVehicles;

	/**
	 * holds all current accidents
	 * (xsd -> Accidentinformation)
	 */
	private final ConcurrentHashMap<SegmentIdentifier, AccidentImmutable> allAccidents;
	
	//protected BlockingQueue<Tuple> waitingList;
	
	private OutputCollector collector;

	/**
	 * current minute of novlavs in allNovLavs
	 */
	//protected ConcurrentHashMap<String, Integer> currentNovLavMinutes;
	

	StopWatch timer;
	
	boolean printed = false;

	private final int processed_xway;

	String tollAssessment;
	String tollNotification;
	
	String tmpname; 

	public TollNotificationBolt(StopWatch timer, int xway) {
		allNovLavs = new ConcurrentHashMap<SegmentIdentifier, NovLav>();
		allVehicles = new ConcurrentHashMap<Integer, VehicleInfo>();
		allAccidents = new ConcurrentHashMap<SegmentIdentifier, AccidentImmutable>();
		//currentNovLavMinutes = new ConcurrentHashMap<String, Integer>();
		//waitingList = new LinkedBlockingQueue<Tuple>();
		this.timer = timer;
		processed_xway = xway;
		tollAssessment = "TollAssessment_" + processed_xway;
		tollNotification = "TollNotification_" + processed_xway;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		tmpname = context.getThisComponentId()+context.getThisTaskId();
		LOG.info("Component "+tmpname+ " subscribed to: "+ context.getThisSources().keySet().toString() );
		
	}

	@SuppressWarnings("unchecked")
	@Override
	public void execute(Tuple tuple) {
		
		String task ="";
		//timer.start();
			
		if (tuple.contains("PosReport")){
			task="position report";
			calcTollAndEmit(tuple);
				
		}else if (tuple.contains("lav")){
			task="novlav";
			updateNovLavs(tuple);
			
			

		}else if (tuple.contains("accidentInfo")){
			task="accident";
			updateAccidents(tuple);
		}

		collector.ack(tuple);
		/*timer.stop();
		if(timer.getDurationTimeSecs()>5)
			LOG.info("TOLLN: " +timer.getElapsedTime()+"ms needed to procss "+task+" tuple:"+tuple);
		else
			LOG.info("TOLLN: " +timer.getElapsedTime()+"ms needed to procss "+task+" tuple:"+tuple);
		*/
	}

	private void updateNovLavs(Tuple tuple) {
		int xWay = tuple.getInteger(0); //@TODO:
		int seg = tuple.getInteger(1);
		int dir = tuple.getInteger(2);
		SegmentIdentifier segmentIdentifier = new SegmentIdentifier(xWay, seg, dir);
		int min = tuple.getIntegerByField("minute");

		NovLav novlav = new NovLav(tuple.getIntegerByField("nov"),
			tuple.getDoubleByField("lav"),
			tuple.getIntegerByField("minute"));
		
		allNovLavs.put(segmentIdentifier, novlav);
		//currentNovLavMinutes.put(tuple.getStringByField("xsd"), min);
		if(LOG.isDebugEnabled())
			LOG.debug("TOLLN:updated novlavs for " +tuple.getStringByField("xsd")+" "+novlav);
	}

	private void updateAccidents(Tuple tuple) {
		int xway = tuple.getInteger(0); //@TODO: unify model with dependency between each bolt
		int seg = tuple.getInteger(1);
		int dir = tuple.getInteger(2);
		SegmentIdentifier xsd = new SegmentIdentifier(xway, seg, dir);
		
		
		AccidentImmutable info = (AccidentImmutable) tuple.getValueByField("accidentInfo");
		if(LOG.isDebugEnabled())
			LOG.debug("TOLLN: recieved accident info: " + info);
	
		if(info.isOver()){
			allAccidents.remove(xsd);
			if(LOG.isDebugEnabled())
				LOG.debug("TOLLN:: removed accident: "+ info);
		}
		else{
			AccidentImmutable prev = allAccidents.put(xsd, info);
			if (prev != null)
				if(LOG.isDebugEnabled())
					LOG.debug("TOLLN: accident (prev: "+prev+")");
			else {
				if(LOG.isDebugEnabled())
					LOG.debug("TOLLN:: added new accident");
			}
		}
	}

	void calcTollAndEmit(Tuple tuple){
		
		PosReport pos = (PosReport) tuple.getValueByField("PosReport");
		SegmentIdentifier segmentTriple = new SegmentIdentifier(
					pos.getSegmentIdentifier().getxWay(), 
			pos.getSegmentIdentifier().getSegment(), 
			pos.getSegmentIdentifier().getDirection());
		
		if (assessTollAndCheckIfTollNotificationRequired(pos)) {
			
			int toll = calcToll(segmentTriple, Time.getMinute(pos.getTime()));
			double lav = 0.0;
			int nov = 0;
			if (allNovLavs.containsKey(segmentTriple)){
				lav = allNovLavs.get(segmentTriple).getLav();
				nov =  allNovLavs.get(segmentTriple).getNov();
			}
			
			String notification = allVehicles.get(pos.getVehicleIdentifier()).getTollNotification(lav, toll, nov);
			if(LOG.isDebugEnabled() && toll>0)
				LOG.debug("TOLLN: actually calculated toll (="+ toll+") for vid=" + pos.getVehicleIdentifier() + " at "+ pos.getTime()+" sec");
			
			if(notification.isEmpty()){
				LOG.info("TOLLN: duplicate toll:" + allVehicles.get(pos.getVehicleIdentifier()).toString());
				
			}else
				collector.emit(tollNotification, new Values(notification));
				//collector.emit(tollNotification,tuple, new Values(notification));
			
		}//else{
			//to check if all positionreports are handled we emit an empty 
			//notification for every report which does not require a toll notification
			//String notification = allVehicles.get(pos.getVid()).getEmptyNotification("sameSegOrExit");
			//collector.emit(tollNotification,tuple, new Values(notification));
		//}
			
	}
	
	/**
	 * Calculate the toll amount according to the current congestion of the segment
	 * @param position
	 * @param minute
	 * @return toll amount to charge the vehicle with
	 */
	 protected int calcToll(SegmentIdentifier position, int minute) {
	    	int toll = DRIVE_EASY;
	    	int nov = 0;
	    	if (allNovLavs.containsKey(position)) {
	    		int novLavMin = allNovLavs.get(position).getMinute();
	    		if(novLavMin==minute || novLavMin+1== minute) 
	    			nov = allNovLavs.get(position).getNov();
	    		else{
	    			if(LOG.isDebugEnabled())
	    				LOG.debug("The novLav is not available or up to date: "
	    				+ allNovLavs.get(position) + "current minute" + minute);
	    		}
		}
	    	
	    	if (tollConditionSatisfied(position, minute)) {
	    		toll = (int) (2 * Math.pow(nov - 50, 2));
	    	}
	    	
	    	
	    	return toll;
		}
	

	
	/**
	 * Assess toll of previous notification and check if tollnotification is
	 * required and in the course update the vehilces information
	 * 
	 * @param posReport
	 * @return true if tollnotification is required
	 */
	protected boolean assessTollAndCheckIfTollNotificationRequired(PosReport posReport) {
    	boolean segmentChanged;
    	if (!allVehicles.containsKey(posReport.getVehicleIdentifier())) {
    		segmentChanged = true;
    		allVehicles.put(posReport.getVehicleIdentifier(), new VehicleInfo(posReport));
    	} else {
    		VehicleInfo vehicle = allVehicles.get(posReport.getVehicleIdentifier());
    		SegmentIdentifier oldPosition = vehicle.getSegmentIdentifier();
    		SegmentIdentifier newPosition = posReport.getSegmentIdentifier();
    		segmentChanged = !oldPosition.equals(newPosition);
    		if(LOG.isDebugEnabled())
    			LOG.debug("TOLLN: assess toll:" + vehicle);
    		//assess previous toll by emitting toll info to be processed by accountbaancebolt
    		collector.emit(tollAssessment, new Values(posReport.getVehicleIdentifier(), vehicle.getXway(),vehicle.getToll(), posReport));

    		vehicle.updateInfo(posReport);
    	}
    	
    	return segmentChanged && !posReport.isOnExitLane();
    }
	
	/**
	 * Check if the condition for charging toll, which depends on the
	 * minute and segment of the vehicle, are given
	 * 
	 * @param segment
	 * @param minute
	 * @return
	 */
	protected boolean tollConditionSatisfied(SegmentIdentifier segment, int minute) {
		double segmentSpeed = 0;
		int carsOnSegment = 0;
		if (allNovLavs.containsKey(segment) && allNovLavs.get(segment).getMinute()==minute) {
			segmentSpeed = allNovLavs.get(segment).getLav();
			carsOnSegment = allNovLavs.get(segment).getNov();
		}
		
		boolean isAccident = false;
		if (allAccidents.containsKey(segment)) {
			isAccident = allAccidents.get(segment).active(minute);
		}
		if(LOG.isDebugEnabled())
		LOG.debug(segment + " => segmentSpeed: " + segmentSpeed + "\tcarsOnSegment: " + carsOnSegment);
    	
    	return segmentSpeed < MAX_SPEED_FOR_TOLL && carsOnSegment > MIN_CARS_FOR_TOLL && !isAccident;    		
	}
	
	
	@Override
	public void cleanup() {
		timer.stop();
		System.out.println("TollNotificationBolt was running for "
				+ timer.getElapsedTimeSecs() + " seconds.");
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	
	
		declarer.declareStream(tollAssessment, new Fields("vid", "xway", "tollAssessed","PosReport"));

		declarer.declareStream(tollNotification, new Fields("tollnotification"));

	}

}

