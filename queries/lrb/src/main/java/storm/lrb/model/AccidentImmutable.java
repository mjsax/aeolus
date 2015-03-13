package storm.lrb.model;

import java.io.Serializable;
import java.util.HashSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.lrb.bolt.SegmentIdentifier;





/**
 * Immutable version of the Accident object for serialization.
 */
public class AccidentImmutable implements Serializable {
	
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(AccidentImmutable.class);
	private int startTime;
	private int startMinute;
	private int lastUpdateTime;
	private int position;
	private boolean over = false;
	private HashSet<SegmentIdentifier> involvedSegs = new HashSet<SegmentIdentifier>();
	private HashSet<Integer> involvedCars = new HashSet<Integer>();
	private int maxPos;
	private int minPos;
	
	public AccidentImmutable() {
		
	}
	
	public AccidentImmutable(Accident accident) {
		this.startTime = accident.getStartTime();
		this.startMinute = Time.getMinute(this.startTime);
		this.position = accident.getAccidentPosition();
		this.lastUpdateTime = accident.getLastUpdateTime();
		this.involvedSegs = accident.getInvolvedSegs();
		this.involvedCars = accident.getInvolvedCars();
		this.over = accident.isOver();
	}
	
	public boolean active(int minute) {
		if(this.isOver()) {
			return minute <= Time.getMinute(this.lastUpdateTime);
		} else {
			return minute > this.startMinute;
		}
	}
	
	public HashSet<Integer> getInvolvedCars() {
		return this.involvedCars;
	}
	
	public int getAccidentPosition() {
		return this.position;
	}
	
	public String getAccNotification(PosReport pos) {
		
		String notification = "1," + pos.getTime() + "," + pos.getEmitTime() / 1000 + "," + pos.getVidAsString() + ","
			+ (this.position / 5280) + "***" + pos.getTime() + "," + pos.getProcessingTime() + "###" + pos.toString()
			+ "###";
		
		if(pos.getProcessingTimeSec() > 5) {
			LOG.error("Time Requirement not met: " + pos.getProcessingTimeSec() + " for " + pos + "\n" + notification);
			if(LOG.isDebugEnabled()) {
				throw new IllegalArgumentException("Time Requirement not met:" + pos + "\n" + notification);
			}
		}
		return notification;
		
	}
	
	public boolean isOver() {
		return this.over;
	}
	
	@Override
	public String toString() {
		return "Accident [startTime=" + this.startTime + ", startMinute=" + this.startMinute + ", lastUpdateTime="
			+ this.lastUpdateTime + ", position=" + this.position + ", over=" + this.over + ", involvedSegs="
			+ this.involvedSegs + ", involvedCars=" + this.involvedCars + ", maxPos=" + this.maxPos + ", minPos="
			+ this.minPos + "]";
	}
	
}
