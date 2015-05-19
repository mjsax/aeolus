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
import java.util.HashSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.lrb.bolt.SegmentIdentifier;
import storm.lrb.tools.Constants;





/**
 * The {@code Accident} class serves to record both current and historical data about accidents.
 * 
 * @author richter
 */
public class Accident implements Serializable {
	
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(Accident.class);
	private long startTime;
	/**
	 * timestamp of last positionreport indicating that the accident is still active
	 */
	private volatile long lastUpdateTime = Integer.MAX_VALUE - 1;
	private int position = -1;
	private boolean over = false;
	private final HashSet<SegmentIdentifier> involvedSegs = new HashSet<SegmentIdentifier>();
	private final HashSet<Integer> involvedCars = new HashSet<Integer>();
	
	// private int maxPos = -1;
	// private int minPos = -1;
	
	protected Accident() {}
	
	public Accident(PosReport report) {
		this.startTime = report.getTime();
		this.position = report.getPosition();
		this.lastUpdateTime = report.getTime();
		this.assignSegments(report.getSegmentIdentifier().getxWay(), report.getPosition(), report
			.getSegmentIdentifier().getDirection());
	}
	
	/**
	 * assigns segments to accidents according to LRB req. (within 5 segments upstream)
	 * 
	 * @param xway
	 *            of accident
	 * @param pos
	 *            of accident
	 * @param dir
	 *            of accident
	 */
	private void assignSegments(int xway, int pos, int dir) {
		
		int segment = pos / Constants.MAX_NUMBER_OF_POSITIONS;
		
		if(dir == 0) {
			// maxPos = pos; minPos = (segment-4)*5280;
			for(int i = segment; 0 < i && i > segment - 5; i--) {
				SegmentIdentifier segmentTriple = new SegmentIdentifier(xway, i, dir);
				this.involvedSegs.add(segmentTriple);
			}
		} else {
			// minPos = pos; maxPos = (segment+5)*5280-1;
			for(int i = segment; i < segment + 5 && i < 100; i++) {
				SegmentIdentifier segmentTriple = new SegmentIdentifier(xway, i, dir);
				this.involvedSegs.add(segmentTriple);
			}
		}
		
		LOG.debug("ACC:: assigned segments to accident: " + this.involvedSegs.toString());
	}
	
	public HashSet<SegmentIdentifier> getInvolvedSegs() {
		return this.involvedSegs;
	}
	
	protected void recordUpdateTime(int curTime) {
		this.lastUpdateTime = curTime;
	}
	
	public boolean active(int minute) {
		return (minute > this.startTime / 60 && minute <= (Time.getMinute(this.lastUpdateTime)));
	}
	
	/**
	 * Checks if accident
	 * 
	 * @param minute
	 * @return
	 */
	public boolean over(int minute) {
		return (minute > (this.lastUpdateTime + 1));
	}
	
	/**
	 * update accident information (includes updating time and adding vid of current position report if not already
	 * present
	 * 
	 * @param report
	 *            positionreport of accident car
	 */
	public void updateAccident(PosReport report) {
		// add car id to involved cars if not there yet
		this.involvedCars.add(report.getVehicleIdentifier());
		this.lastUpdateTime = report.getTime();
		
	}
	
	/**
	 * add car ids of involved cars to accident info
	 * 
	 * @param vehicles
	 *            hashset wtih vids
	 */
	public void addAccVehicles(HashSet<Integer> vehicles) {
		this.involvedCars.addAll(vehicles);
	}
	
	/**
	 * get all vids of vehicles involved in that accident
	 * 
	 * @return vehicles hashset wtih vids
	 */
	public HashSet<Integer> getInvolvedCars() {
		return this.involvedCars;
	}
	
	/**
	 * get accident position
	 * 
	 * @return position number
	 */
	public int getAccidentPosition() {
		return this.position;
	}
	
	/*
	 * private boolean isInFrontAcc(Integer pos) { if( minPos<= pos && pos <= maxPos) return false; else return true; }
	 */
	public void setOver() {
		this.over = true;
	}
	
	public void setOver(long timeinseconds) {
		this.over = true;
	}
	
	public boolean isOver() {
		return this.over;
	}
	
	public long getStartTime() {
		return this.startTime;
	}
	
	public long getLastUpdateTime() {
		return this.lastUpdateTime;
	}
	
}
