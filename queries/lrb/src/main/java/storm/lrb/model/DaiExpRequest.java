package storm.lrb.model;

import java.io.Serializable;

import storm.lrb.tools.StopWatch;





/**
 * Object to represent daily expnditure requests
 */
/*
 * internal implementation notes: - does not implement clone because Values doesn't
 */
@SuppressWarnings("CloneableImplementsClone")
public class DaiExpRequest extends LRBtuple implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	public DaiExpRequest() {
		super();
		
	}
	
	public DaiExpRequest(String tupel, StopWatch time) {
		super(tupel, time);
		
	}
	
	@Override
	public String toString() {
		return "ExpenditureReq [time=" + this.getTime() + ", vid=" + this.getVehicleIdentifier() + ", xway="
			+ this.getSegmentIdentifier().getxWay() + ", qid=" + this.getQueryIdentifier() + ", day=" + this.getDay()
			+ "]";
	}
	
}
