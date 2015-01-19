package storm.lrb.model;

import java.io.Serializable;
import java.util.HashSet;

import org.apache.log4j.Logger;

public class Accident implements Serializable {
	
	private static final long serialVersionUID = 1L;
	private int startTime;
	private int startMinute;
	private int endMinute;
	/**
	 * timestamp of last positionreport indicating that 
	 * the accident is still active
	 */
	private volatile int lastUpdateTime = Integer.MAX_VALUE - 1;
	private int position = -1;
	private boolean over = false;
	private HashSet<String> involvedSegs = new HashSet<String>();
	private HashSet<Integer> involvedCars = new HashSet<Integer>();
	//private int maxPos = -1;
	//private int minPos = -1;
	
	private static final Logger LOG = Logger.getLogger(Accident.class);
	
	public Accident() {

	}
	public Accident(PosReport report) {
		startTime = report.getTime();
		startMinute = Time.getMinute(startTime);
		position = report.getPos();
		lastUpdateTime = report.getTime();
		assignSegments(report.getXway(), report.getPos(), report.getDir());
	}

	
	/**
	 * assigns segments to accidents according to LRB req.
	 * (within 5 segments upstream)
	 * @param xway of accident 
	 * @param pos of accident
	 * @param dir of accident
	 */
	protected void assignSegments(int xway, int pos, int dir) {
	
		int segment = pos/5280;
		
		if(dir==0){
			//maxPos = pos; minPos = (segment-4)*5280;
		for (int i = segment;0 < i && i > segment - 5 ; i--) {
			String string = xway+"-"+i+"-"+dir;
			involvedSegs.add(string);
		}}else{
			//minPos = pos; maxPos = (segment+5)*5280-1;
			for (int i = segment; i < segment+5 && i < 100 ; i++) {
				String string = xway+"-"+i+"-"+dir;
				involvedSegs.add(string);
			}
		}
		
		LOG.debug("ACC:: assigned segments to accident: "+ involvedSegs.toString());
	}
	
	public HashSet<String> getInvolvedSegs(){
		return involvedSegs;
	}


	protected void recordUpdateTime(int curTime) {
		this.lastUpdateTime = curTime;
	}

	public boolean active(int minute) {
		return (minute > (int)startTime/60 && minute <= (Time.getMinute(lastUpdateTime)));
	}

	/**
	 * Checks if accident 
	 * @param minute
	 * @return
	 */
	public boolean over(int minute) {
		return (minute > (lastUpdateTime + 1));
	}

	/**
	 * update accident information (includes updating time and adding vid of
	 * current position report if not already present
	 * 
	 * @param report positionreport of accident car
	 */
	public void updateAccident(PosReport report) {
		//add car id to involved cars if not there yet
		this.involvedCars.add(report.getVid());
		this.lastUpdateTime = report.getTime();
		
	}
	
	/**
	 * add car ids of involved cars to accident info
	 * @param vehicles hashset wtih vids
	 */
	public void addAccVehicles(HashSet<Integer> vehicles) {	
		this.involvedCars.addAll(vehicles);
	}
	
	/**
	 *  get all vids of vehicles involved in that accident
	 * @return vehicles hashset wtih vids
	 */
	public HashSet<Integer> getInvolvedCars(){
		return involvedCars;
	}
	
	/**
	 * get accident position
	 * @return position number
	 */
	public int getAccidentPosition(){
		return position;
	}
	
	/*private boolean isInFrontAcc(Integer pos) {
		if( minPos<=  pos && pos <= maxPos)
		return false;
		else return true;
	}*/
	
	public void setOver() {
		over = true;
	}
	public void setOver(int timeinseconds) {
		endMinute = Time.getMinute(timeinseconds);
		over = true;
	}
	public final boolean isOver() {
		return over;
	}
	
	public int getStartTime() {
		return startTime;
	}
	public int getLastUpdateTime() {
		return lastUpdateTime;
	}
	
	@Override
	public String toString() {
		return "Accident [startTime=" + startTime + ", startMinute="
				+ startMinute + ", endMinute=" + endMinute
				+ ", lastUpdateTime=" + lastUpdateTime + ", position="
				+ position + ", over=" + over + ", involvedSegs="
				+ involvedSegs + ", involvedCars=" + involvedCars + "]";
				//", maxPos=" + maxPos + ", minPos=" + minPos + "]";
	}
	
	
}
