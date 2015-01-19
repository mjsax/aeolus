package storm.lrb.model;

import java.io.Serializable;

import org.apache.commons.collections.map.MultiKeyMap;
import org.apache.log4j.Logger;

/**
 * Object representing a vehicle
 */
public class VehicleInfo  implements Serializable {
//TODO evtl unterschiedliche vehicle objekte zusammen tun
	private static final long serialVersionUID = 1L;
	
	private static final Logger LOG = Logger.getLogger(VehicleInfo.class);

	
	private Integer vid;
	private int calculatedToll = 0;
	
	private int day = 70;

	
	private PosReport posreport;
	
	/**
	 * TODO load toll history of vehicle 
	 * keeps toll history as DAy,XWAY,Tolls
	 */
	private MultiKeyMap tollHistory = new MultiKeyMap();
	
	/**
	 * Time of positionreport which was last processed
	 */
	private long lastSendToll=-1;
	
	
	
	public VehicleInfo(Integer vid) {
		this.vid = vid;
	}

	public VehicleInfo() {
		
	}
	
	public VehicleInfo(PosReport pos) {
		
		posreport = pos;	
		vid = pos.getVid();
		
		
	}
	
	/**
	 * Update vehicle information with new position report
	 * @param pos position report
	 */
	public void updateInfo(PosReport pos){
		posreport = pos;
		
	}

	public int getToll() {
		return calculatedToll;
	}


	public void setToll(int toll) {
		this.calculatedToll = toll;
	}

	


	public String getXsd() {
		return posreport.getXsd();
	}


	public long getLastReportTime(){
		return lastSendToll;
	}


	public Integer getXway() {
		return posreport.getXway();
	}


	

	/**
	 * can be used as a placeholder for reacting to postion reports which do not
	 * cause emission of toll notifications
	 * @param why reason to fill blank space
	 * @return notification
	 */
	public String getEmptyNotification(String why) {

		String notification = why + "***" + posreport.getTime() + ","
				+ posreport.getProcessingTime() + "###" + posreport.toString()
				+ "###";
		return notification;
	}
	
	public String getTollNotification(double lav, int toll, int nov) {
		
		setToll(toll);
		
		//in case of storm replaying of tuples:
		// check if we have duplicate processing of position report? 
		if(posreport.getTime()==lastSendToll){
			return "";//getEmptyNotification("duplicate");
		}
		String notification = "0," + vid + "," + posreport.getTime()  + ","
				+ posreport.getEmitTime() + "," + (int) lav + "," + toll
				+ "***" + posreport.getTime() + ","
				+ posreport.getProcessingTime() + "###" + posreport.toString()
				+ "###"+nov;

		//check if time requirements are met if not stop computation
		long diff;
		if(posreport.getProcessingTimeSec()>5){
			LOG.error("Time Requirement not met: "+posreport.getProcessingTimeSec()+ " for "+posreport+"\n"+notification);
			if(LOG.isDebugEnabled())
				throw new Error("Time Requirement not met:"+posreport+"\n"+notification);
		}
		lastSendToll = posreport.getTime();
		return notification;
	}


	@Override
	public String toString() {
		return "VehicleInfo [vid=" + vid + ", calculatedToll=" + calculatedToll
				+ ", xway=" + getXway() + ", day="
				+ day + ", time=" + getLastReportTime() + ", tollHistory=" + tollHistory
				+ ", segDir=" + getXsd()+ "]";
	}

	
}
