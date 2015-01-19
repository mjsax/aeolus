package storm.lrb.model;

import java.io.Serializable;

import storm.lrb.tools.StopWatch;
/**
 * object to represent time travel requests
 */
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
		return "TTimeReq [time=" + getTime() + ", vid=" + getVehicleIdentifier()+ ", xway=" + getSegmentIdentifier().getxWay()
				+ ", qid=" + getQueryIdentifier() + ", sInit=" + getSinit()
				+ ", sEnd=" + getSend() + ", dow=" + getDow() + ", tod=" + getTod() + "]";
	}

}
