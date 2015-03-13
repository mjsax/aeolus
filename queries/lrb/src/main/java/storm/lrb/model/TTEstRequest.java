package storm.lrb.model;

import java.io.Serializable;

import storm.lrb.tools.StopWatch;





/**
 * object to represent time travel requests
 */
/*
 * internal implementation notes: - does not implement clone because Values doesn't
 */
@SuppressWarnings("CloneableImplementsClone")
public class TTEstRequest extends LRBtuple implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	public TTEstRequest() {
		super();
		
	}
	
	public TTEstRequest(String tupel, StopWatch time) {
		super(tupel, time);
		
	}
	
	@Override
	public String toString() {
		return "TTimeReq [time=" + this.getTime() + ", vid=" + this.getVehicleIdentifier() + ", xway="
			+ this.getSegmentIdentifier().getxWay() + ", qid=" + this.getQueryIdentifier() + ", sInit="
			+ this.getSinit() + ", sEnd=" + this.getSend() + ", dow=" + this.getDow() + ", tod=" + this.getTod() + "]";
	}
	
}
