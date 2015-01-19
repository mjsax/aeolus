package storm.lrb.model;

import java.io.Serializable;



/**
 * Object that keeps account information for a vehicle.
 */
public class VehicleAccount  implements Serializable {


	@Override
	public String toString() {
		return "VehicleAccount [vid=" + vid + ", tollToday=" + tollToday
				+ ", tollTime=" + tollTime + ", day=" + day + ", xWay=" + xWay
				+ "]";
	}

	private static final long serialVersionUID = 1L;
	private Integer vid = 0;
	private int tollToday = 0;
	private Long tollTime = 0L;

	private int day = 70;
	private Integer xWay;

	public VehicleAccount() {
	}
	
	
	
	public VehicleAccount(Integer vid, Integer xWay) {
		this.vid = vid;
		this.xWay = xWay;
		//TODO checken ob er an einem tag nur ein xway belegen kann, evtl auch rausnehmen
	}
	

	public VehicleAccount(int calculatedToll, PosReport pos) {
		this.vid = pos.getVid();
		this.xWay = pos.getXway();
		
		assessToll(calculatedToll, pos.getEmitTime());
	}

	public VehicleAccount(AccBalRequest bal) {
		this.vid = bal.getVid();
		this.xWay = bal.getXway();
	}


	/**
	 * Adds newly assesed toll to the current account.
	 * @param calculatedToll amount
	 * @param time of assesment
	 */
	public void assessToll(int calculatedToll, Long time){
		
		tollToday += calculatedToll;
		tollTime = time;
		
	}
	

	
	public String getAccBalanceNotification(AccBalRequest accBalReq) {
		// TODO nach zweiter meinung fragen: Benchmarkspezifikation
		// widerspricht sich bei der Reihenfolge der Werte des Outputtuples.
		
		String notification = "2," + accBalReq.getTime() + ","
				+ accBalReq.getEmitTime() + "," + accBalReq.getQid() + ","
				+ tollTime / 1000 +","+ tollToday + "***"
				+ accBalReq.getTime() + "," + accBalReq.getProcessingTime()
				+ "###" + this.toString() + "###";
		return notification;
	}


	




	
}
