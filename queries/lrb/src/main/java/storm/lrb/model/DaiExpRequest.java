package storm.lrb.model;

import java.io.Serializable;

import storm.lrb.tools.StopWatch;
/**
 * Object to represent daily expnditure requests
 */
public class DaiExpRequest extends LRBtuple implements Serializable{

	
	private static final long serialVersionUID = 1L;
	
	public DaiExpRequest() {
		super();

	}

	public DaiExpRequest(String tupel, StopWatch time) {
		super(tupel, time);

	}

	@Override
	public String toString() {
		return "ExpenditureReq [time=" + getTime() + ", vid=" + getVid() + ", xway=" + getXway()
				+ ", qid=" + getQid() + ", day=" + getDay() + "]";
	}

}
