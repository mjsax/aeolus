package storm.lrb.model;

import java.util.HashMap;
import java.util.Map;





/**
 * Helper class that computes statistics associated with one segment, over one minute.
 * 
 * @author richter
 */
public class MinuteStatistics {
	
	private final Map<Integer, Integer> vehicleSpeeds = new HashMap<Integer, Integer>();
	private double speedAverage; // rolling average for vehicles in this segment
	
	protected synchronized void addVehicleSpeed(int vehicleId, int vehicleSpeed) {
		double cumulativeSpeed = this.speedAverage * this.vehicleSpeeds.size();
		if(this.vehicleSpeeds.containsKey(vehicleId)) {
			int prevVehicleSpeed = this.vehicleSpeeds.get(vehicleId);
			cumulativeSpeed -= prevVehicleSpeed;
			cumulativeSpeed += (prevVehicleSpeed + vehicleSpeed) / 2.0;
		} else {
			this.vehicleSpeeds.put(vehicleId, vehicleSpeed);
			cumulativeSpeed += vehicleSpeed;
		}
		
		this.speedAverage = cumulativeSpeed / this.vehicleSpeeds.size();
	}
	
	protected synchronized double speedAverage() {
		return this.speedAverage;
	}
	
	protected synchronized int vehicleCount() {
		return this.vehicleSpeeds.size();
	}
	
	@Override
	public String toString() {
		return " [vehicleSpeeds=" + this.vehicleSpeeds + ", speedAverage=" + this.speedAverage + "]";
	}
}
