package storm.lrb.model;

import java.io.Serializable;
import java.util.HashSet;

import org.apache.log4j.Logger;

/**
 * Immutable version of the Accident object for serialization.
 */
public class AccidentImmutable implements Serializable {
	
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = Logger.getLogger(AccidentImmutable.class);
	private int startTime;
	private int startMinute;
	private int lastUpdateTime;
	private int position;
	private boolean over = false;
	private HashSet<String> involvedSegs = new HashSet<String>();
	private HashSet<Integer> involvedCars = new HashSet<Integer>();
	private int maxPos;
	private int minPos;
	
	
	public AccidentImmutable() {

	}
	public AccidentImmutable(Accident accident) {
		startTime = accident.getStartTime();
		startMinute = Time.getMinute(startTime);
		position = accident.getAccidentPosition();
		lastUpdateTime = accident.getLastUpdateTime();
		involvedSegs = accident.getInvolvedSegs();
		involvedCars = accident.getInvolvedCars();
		over = accident.isOver();
	}

	
	public boolean active(int minute) {
		if(isOver())
			return minute <= Time.getMinute(lastUpdateTime);
		else
			return minute > startMinute;
	}

	
	
	public HashSet<Integer> getInvolvedCars(){
		return involvedCars;
	}
	
	public int getAccidentPosition(){
		return position;
	}
	public String getAccNotification(PosReport pos) {
	
		String notification = "1,"+pos.getTime()+","+ pos.getEmitTime()/1000+","
							+pos.getVidAsString() +","+((int)position/5280)
							+"***"+pos.getTime()+","+pos.getProcessingTime() 
							+ "###"+pos.toString()+"###";
		
		if(pos.getProcessingTimeSec()>5){
			LOG.error("Time Requirement not met: "+pos.getProcessingTimeSec()+ " for "+pos+"\n"+notification);
			if(LOG.isDebugEnabled())
				throw new Error("Time Requirement not met:"+pos+"\n"+notification);
		}
		return notification;
		
	}
	
	public final boolean isOver() {
		return over;
	}
	@Override
	public String toString() {
		return "Accident [startTime=" + startTime + ", startMinute="
				+ startMinute 
				+ ", lastUpdateTime=" + lastUpdateTime + ", position="
				+ position + ", over=" + over + ", involvedSegs="
				+ involvedSegs + ", involvedCars=" + involvedCars + ", maxPos="
				+ maxPos + ", minPos=" + minPos + "]";
	}
	
	
}
