package storm.lrb.model;

import java.io.Serializable;

import storm.lrb.tools.StopWatch;
import sun.font.CreatedFontTracker;

/**
 * object to represent position reports
 *
 */
public class PosReport extends LRBtuple implements Serializable {

	private static final long serialVersionUID = 1L;

	public PosReport() {
		super();
	}

	/**
	 * Creates a PosReport from a the trailing part of a LRB input line 
	 * after the type has been removed
	 * @param tupel
	 * @param time 
	 */
	public PosReport(String tupel, StopWatch time) {
		super(LRBtuple.TYPE_POSITION_REPORT, tupel, time);

	}

	public String getXsd() {
		return getXway() + "-" + getSeg() + "-" + getDir();
	}

	
	@Override
	public String toString() {
		return "PosReport on " + getXsd() + " [time=" + getTime() + ", vid="
				+ getVid() + ", spd=" + getSpd() + ", lane=" + getLane() + ", dir=" + getDir()
				+ ", pos=" + getPos() + "(Created: "+this.getCreated()+" Duration: "
				+ getProcessingTime() + " ms, StormTimer: "
				+ getStormTimer().getElapsedTimeSecs() + "s)]";
	}

	public String getXD() {
		return getXway() + "-" + getDir();
		
	}

	public boolean isOnExitLane() {
		return getLane() == 4;
	}

}
