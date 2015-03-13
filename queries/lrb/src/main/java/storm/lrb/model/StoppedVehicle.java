/*
 * #!
 * %
 * Copyright (C) 2014 - 2015 Humboldt-Universität zu Berlin
 * %
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #_
 */
package storm.lrb.model;

import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;





/**
 * Helper class to count stops of a vehicle.
 */
public class StoppedVehicle implements Serializable {
	
	private static final Logger LOG = LoggerFactory.getLogger(StoppedVehicle.class);
	
	private static final long serialVersionUID = 1L;
	private final int vid;
	private int cnt = 0;
	private int time;
	private PosReport firstReport;
	private int position;
	
	// for serialization only
	public StoppedVehicle() {
		this.time = -1;
		this.vid = -1;
		this.firstReport = null;
		this.position = -1;
		
	}
	
	public StoppedVehicle(PosReport report) {
		this.vid = report.getVehicleIdentifier();
		this.time = report.getTime();
		this.firstReport = report;
		this.position = report.getPosition();
	}
	
	public int getPosition() {
		return this.position;
	}
	
	/**
	 * checks if still stopped and records the stop and returns the number of how many consecutive stops this car has
	 * reported
	 * 
	 * @param curReport
	 * @return the number of consecutives stops this vehicle reported
	 */
	public int recordStop(PosReport curReport) {
		if(!this.stillStopped(curReport)) {
			this.cnt = 1;
			this.firstReport = curReport;
			this.time = curReport.getTime();
			this.firstReport = curReport;
			this.position = curReport.getPosition();
		} else {
			this.cnt++;
		}
		
		return this.cnt;
	}
	
	/**
	 * check if new position report of car indicates that the car is still stopped
	 * 
	 * @param pos
	 *            new position report
	 * @return true if
	 */
	protected boolean stillStopped(PosReport pos) {
		
		if(!(pos instanceof PosReport)) {
			return false;
		}
		if(pos.getTime() == this.firstReport.getTime()) {
			// LOG.info("identischer posreport");
			return true;
		} else {
			return (pos.getSegmentIdentifier().getxWay() == this.firstReport.getSegmentIdentifier().getxWay() && pos
				.getSegmentIdentifier().getDirection() == this.firstReport.getSegmentIdentifier().getDirection())
				&& pos.getPosition().equals(this.firstReport.getPosition())
				&& pos.getTime() <= ((30 * this.cnt) + this.firstReport.getTime());
		}
	}
	
	public int getVid() {
		return this.vid;
	}
	
	@Override
	public String toString() {
		return "StoppedVehicle [vid=" + this.vid + ", cnt=" + this.cnt + ", time=" + this.time + ", firstReport="
			+ this.firstReport + "]";
	}
	
}
