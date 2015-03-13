package storm.lrb.model;

import java.io.Serializable;

import storm.lrb.tools.StopWatch;





/**
 * object to represent position reports. Every vehicle emits a position report every
 * 
 * LAV = latest average velocity (
 */
/*
 * internal implementation notes: - does not implement clone because Values doesn't
 */
@SuppressWarnings("CloneableImplementsClone")
public class PosReport extends LRBtuple implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	public PosReport() {
		super();
	}
	
	/**
	 * Creates a PosReport from a the trailing part of a LRB input line after the type has been removed
	 * 
	 * @param tupel
	 * @param time
	 */
	public PosReport(String tupel, StopWatch time) {
		super(LRBtuple.TYPE_POSITION_REPORT, tupel, time);
		
	}
	
	@Override
	public String toString() {
		return "PosReport on " + " [time=" + this.getTime() + ", vid=" + this.getVehicleIdentifier() + ", spd="
			+ this.getCurrentSpeed() + ", lane=" + this.getLane() + ", dir="
			+ this.getSegmentIdentifier().getDirection() + ", pos=" + this.getPosition() + "(Created: "
			+ this.getCreated() + " Duration: " + this.getProcessingTime() + " ms, StormTimer: "
			+ this.getStormTimer().getElapsedTimeSecs() + "s)]";
	}
	
	public boolean isOnExitLane() {
		return this.getLane() == 4;
	}
	
}
