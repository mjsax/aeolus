package storm.lrb.model;

import java.io.Serializable;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.lrb.tools.StopWatch;

/**
 * superclass for all requests 
 *
 */
public class LRBtuple  implements Serializable{
	public final static int TYPE_POSITION_REPORT = 0;
	public final static int TYPE_ACCOUNT_BALANCE = 2;
	public final static int TYPE_DAILY_EXPEDITURE = 3;
	public final static int TYPE_TRAVEL_TIME_REQUEST = 4;
	
	private static final long serialVersionUID = 1L;
	private final static Logger LOGGER = LoggerFactory.getLogger(LRBtuple.class);

	/**
	 * tuple type 0=Position report 2=Account balance requests 3=daily
	 * expenditure request 4=Travel time request
	 */
	private Integer type;
	
	/**
	 * time of creation (in the storm application)
	 */
	private Long created;

	/**
	 * Time (0. . .10799)^3 is a timestamp identifying the time at which the
	 * position report was emitted
	 */
	private int time;

	/**
	 * VID (0. . . MAXINT) is an integer vehicle identifier i
	 */
	private Integer vid;

	/**
	 * Spd (0. . .100) is an integer reﬂecting the speed of the vehicle (in MPH)
	 * at the time the position report
	 */
	private int spd;
	/**
	 * XWay (0. . . L−1) identifies the expressway from which the position
	 * report is emitted
	 */
	private Integer xway;
	/**
	 * Lane (0. . .4) identiﬁes the lane of the expressway from which the
	 * position report is emitted 0 if it is an entrance ramp (ENTRY), 1 − 3 if
	 * it is a travel lane (TRAVEL) and 4 if it is an exit ramp (EXIT).
	 */
	private Integer lane;
	/**
	 * Dir (0. . .1) indicates the direction (0 for Eastbound and 1 for
	 * Westbound)
	 */
	private Integer dir;
	/**
	 * Seg (0. . .99) identiﬁes the mile-long segment from which the position
	 * report is emitted
	 */
	private Integer seg;
	/**
	 * Pos (0. . .527999) identiﬁes the horizontal position of the vehicle as a
	 * measure of the number of feet from the western most point on the
	 * expressway (i.e.,Pos = x)
	 */
	private Integer pos;
	/**
	 * QID is an integer query identiﬁer
	 */
	private Integer qid;
	/**
	 * 
	 */
	private Integer sinit;
	/**
	 * 
	 */
	private Integer send;
	/**
	 * DOW (1. . .7) specify the day of the week
	 */
	private Integer dow;
	/**
	 * TOD (1. . .1440) specifies the day of the week and minute number in the
	 * day when the journey would take place
	 */
	private Integer tod;
	/**
	 * day (1 is yesterday, 69 is 10 weeks ago)
	 */
	private Integer day;

	StopWatch timer;
	
	StopWatch stormTimer = null;

	public LRBtuple(String tupel) {

		timer = new StopWatch(0);

		String[] result = tupel.split(",");
		if (result.length < 15) {
			throw new IllegalArgumentException(
					"Tuple ["+tupel+"] does not match required format");
		}

		type = Integer.valueOf(result[0]);
		time = Integer.valueOf(result[1]);
		timer.setOffset(time);
		
		vid = Integer.valueOf(result[2]);
		spd = Integer.valueOf(result[3]);
		xway = Integer.valueOf(result[4]);
		lane = Integer.valueOf(result[5]);
		dir = Integer.valueOf(result[6]);
		seg = Integer.valueOf(result[7]);
		pos = Integer.valueOf(result[8]);

		qid = Integer.valueOf(result[9]);
		sinit = Integer.valueOf(result[10]);
		send = Integer.valueOf(result[11]);
		dow = Integer.valueOf(result[12]);
		tod = Integer.valueOf(result[13]);
		day = Integer.valueOf(result[14]);
		//timer = new StopWatch(time);
		
		

	}

	public LRBtuple(){
		//kryo needs empty constructor
	}
	
	public LRBtuple(String tupel, StopWatch systemtimer) {
		timer = new StopWatch(0);

		String[] result = tupel.split(",");
		//System.out.println("splitted "+Arrays.toString(result));
		if (result.length < 15) {
			throw new IllegalArgumentException(
					"Tuple does not match required format");
		}

		type = Integer.valueOf(result[0]);
		time = Integer.valueOf(result[1]);
		timer.setOffset(time);

		vid = Integer.valueOf(result[2]);
		spd = Integer.valueOf(result[3]);
		xway = Integer.valueOf(result[4]);
		lane = Integer.valueOf(result[5]);
		dir = Integer.valueOf(result[6]);
		seg = Integer.valueOf(result[7]);
		pos = Integer.valueOf(result[8]);

		qid = Integer.valueOf(result[9]);
		sinit = Integer.valueOf(result[10]);
		send = Integer.valueOf(result[11]);
		dow = Integer.valueOf(result[12]);
		tod = Integer.valueOf(result[13]);
		day = Integer.valueOf(result[14]);
		//timer = new StopWatch(time);
		stormTimer = systemtimer;
		
		created = stormTimer.getElapsedTime();
	}
	
	/**
	 * Creates a tuple of the remainder of a LRB input line after the type 
	 * part of the tuple string has been removed
	 * @param type
	 * @param tupleTail 
	 * @param time 
	 */
	public LRBtuple(int type, String tupleTail, StopWatch time) {
		this.type = type;
		
		timer = new StopWatch(0);

		String[] result = tupleTail.split(",");
		if (result.length < 14) {
			throw new IllegalArgumentException(
					"Tuple ["+tupleTail+"] does not match required format");
		}

		timer= time;
		
		vid = Integer.valueOf(result[1]);
		spd = Integer.valueOf(result[2]);
		xway = Integer.valueOf(result[3]);
		lane = Integer.valueOf(result[4]);
		dir = Integer.valueOf(result[5]);
		seg = Integer.valueOf(result[6]);
		pos = Integer.valueOf(result[7]);

		qid = Integer.valueOf(result[8]);
		sinit = Integer.valueOf(result[9]);
		send = Integer.valueOf(result[10]);
		dow = Integer.valueOf(result[11]);
		tod = Integer.valueOf(result[12]);
		day = Integer.valueOf(result[13]);
	}
	
	public LRBtuple(int type, String tupleTail) {
		this(type, tupleTail, retrieveTimeFromTuple(tupleTail));
	}
	
	private static StopWatch retrieveTimeFromTuple(String tuple) {
		String[] tupleSplit = tuple.split(",");
		if (tupleSplit.length < 1) {
			LOGGER.debug(String.format("tuple line '%s' doesn't contain a valid time value", tuple));
		}
		String timeString = tupleSplit[0];
		long time0 = Long.parseLong(timeString);
		return new StopWatch(time0);
	}

	public Integer getType() {
		return type;
	}

	public int getTime() {
		return time;
	}

	public Integer getVid() {
		return vid;
	}
	
	public String getVidAsString() {
		return vid.toString();
	}

	public int getSpd() {
		return spd;
	}

	public Integer getXway() {
		return xway;
	}

	public Integer getLane() {
		return lane;
	}

	public Integer getDir() {
		return dir;
	}

	public Integer getSeg() {
		return seg;
	}

	public Integer getPos() {
		return pos;
	}

	public Integer getQid() {
		return qid;
	}

	public Integer getSinit() {
		return sinit;
	}

	public Integer getSend() {
		return send;
	}

	public Integer getDow() {
		return dow;
	}

	public Integer getTod() {
		return tod;
	}

	public Integer getDay() {
		return day;
	}

	public StopWatch getTimer() {
		return timer;
	}
	
	/**
	 * Time of creation. (actual running time of the simulation)
	 * @return
	 */
	public Long getCreated() {
		return created;
	}
	public StopWatch getStormTimer() {
		return stormTimer;
	}
	
	/**
	 * get the emit time for notification output
	 * @return (time+processing time)
	 */
	public long getEmitTime() {

		return time + this.getProcessingTimeSec();
	}

	/**
	 * get the time it took to process this tuple in ms
	 * @return processing time in ms
	 */
	public long getProcessingTime() {
		return timer.getDurationTime();
	}
	
	/**
	 * get the time it took to process this tuple in ms
	 * @return processing time in seconds.
	 */
	public long getProcessingTimeSec() {
		return timer.getDurationTimeSecs();
	}

	@Override
	public String toString() {
		return "LRBtuple [type=" + type + ", created=" + created + ", time="
				+ time + ", vid=" + vid + ", timer=" + timer + ", stormTimer="
				+ stormTimer + "]";
	}

	
}
