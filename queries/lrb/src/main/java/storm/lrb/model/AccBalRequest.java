package storm.lrb.model;

import storm.lrb.tools.StopWatch;

/**
 * Object representing account balance requests
 *
 */
public class AccBalRequest extends LRBtuple{

	private static final long serialVersionUID = 1L;

	public AccBalRequest() {
		super();

	}
	public AccBalRequest(String tupel, StopWatch time) {
		super(tupel, time);

	}
	
	
	@Override
	public String toString() {
		return "BalanceReq [time=" + getTime() + ", vid=" + getVehicleIdentifier() + ", qid=" + getQueryIdentifier()+ "]";
	}

}
