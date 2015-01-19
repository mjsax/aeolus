package storm.lrb.bolt;



import storm.lrb.model.PosReport;
import storm.lrb.model.SegmentStatistics;
import storm.lrb.model.Time;
import java.util.Map;
import java.util.Set;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import org.apache.log4j.Logger;



/**
 * This bolt computes the average speed of all cars in a given segment and
 * direction and emits these every minute.
 * 
 * This is the alternative to using AvgsBolt+LavBolt.
 * 
 */
public class SegmentStatsBolt extends BaseRichBolt {

  @Override
	public String toString() {
		return "SegmentStats [segmentStats=" + segmentStats + ", curMinute="
				+ curMinute + "]";
	}

  private static final long serialVersionUID = 5537727428628598519L;
  private static final Logger LOG = Logger.getLogger(SegmentStatsBolt.class);
 
   
  private static final int    START_MINUTE = 0;
  private static final int    AVERAGE_MINS = 5;
  
  
  /**
   * contains all statistical information for each segment and minute
   */
  private final SegmentStatistics segmentStats   = new SegmentStatistics();
  

  private OutputCollector collector;
  private int processed_xway = -1;
  private int curMinute = 0;
  private String tmpname;
   
  public SegmentStatsBolt(int xway) {
	  processed_xway = xway;
  }


 

  @SuppressWarnings("rawtypes")
  @Override
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    this.collector = collector;
    tmpname = context.getThisComponentId()+context.getThisTaskId();
	LOG.info(tmpname+ " mit diesen sources: "+ context.getThisSources().keySet().toString() );
	
  }

  @Override
  public void execute(Tuple tuple) {
	  
	  countAndAck(tuple);
   
  }

  private void emitCurrentWindowCounts() {
	      
	  int prevMinute = Math.max(curMinute-1, START_MINUTE);
	  
	  Set<SegmentIdentifier> segmentList = segmentStats.getXsdList();
	  if(LOG.isDebugEnabled())
		  LOG.debug("Watching the following segments: "+segmentList);

	  //compute the current lav for every segment
		for (SegmentIdentifier xsd : segmentList) {
			int segmentCarCount = 0;
			double speedSum = 0.0;
			int time = Math.max(curMinute - AVERAGE_MINS, 1);
			for (; time <= curMinute; ++time) {
				if (segmentStats.vehicleCount(time, xsd) > 0) {
					segmentCarCount++;
					speedSum += segmentStats.speedAverage(time, xsd);
				}
			}
			double speedAverage = 0.0;
			if (segmentCarCount != 0) {
				speedAverage = (speedSum / segmentCarCount);
			}
			collector.emit(new Values(xsd.getxWay(), xsd.getSegment(), xsd.getDirection(), 
								segmentCarCount, speedAverage, prevMinute));

		}
	  

   
  }


  private void countAndAck(Tuple tuple){
	  	
	  	PosReport pos = (PosReport) tuple.getValueByField("PosReport");
	    
		SegmentIdentifier segment = new SegmentIdentifier(
			pos.getSegmentIdentifier().getxWay(), 
			pos.getSegmentIdentifier().getSegment(), 
			pos.getSegmentIdentifier().getDirection());
	   
	    int newMinute = Time.getMinute(pos.getTime());
	    if(newMinute > curMinute){
	    	emitCurrentWindowCounts();
	    	curMinute = Time.getMinute(pos.getTime());
	    	
	    }
	    segmentStats.addVehicleSpeed(curMinute, segment, pos.getVehicleIdentifier(), pos.getCurrentSpeed());
	    //System.out.println("segmentstats added");
	    collector.ack(tuple);
  }
  
 
  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("xway", "seg", "dir", "nov", "lav", "minute"));
  }

 
}
