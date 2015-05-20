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
import storm.lrb.bolt.SegmentIdentifier;

import storm.lrb.tools.StopWatch;





/**
 * object to represent position reports. Every vehicle emits a position report every
 * 
 * LAV = latest average velocity (
 * 
 * LRB format: (Type = 0, Time, VID, Spd, XWay, Lane, Dir, Seg, Pos)
 */
/*
 * internal implementation notes: - does not implement clone because Values doesn't
 */
@SuppressWarnings("CloneableImplementsClone")
public class PosReport extends LRBtuple implements Serializable {
	
	private static final long serialVersionUID = 1L;
	public static final int DIRECTION_EASTBOUND = 1;
	public static final int DIRECTION_WESTBOUND = 2;
	
	private SegmentIdentifier segmentIdentifier;
	/**
	 * Pos (0. . .527999) identiﬁes the horizontal position of the vehicle as a measure of the number of feet from the
	 * western most point on the expressway (i.e.,Pos = x)
	 */
	private Integer position;
	/**
	 * Spd (0. . .100) is an integer reﬂecting the speed of the vehicle (in MPH) at the time the position report
	 */
	private int currentSpeed;
	/**
	 * Lane (0. . .4) identiﬁes the lane of the expressway from which the position report is emitted 0 if it is an
	 * entrance ramp (ENTRY), 1 − 3 if it is a travel lane (TRAVEL) and 4 if it is an exit ramp (EXIT).
	 */
	private Integer lane;
	private Integer vehicleIdentifier;
	private Integer xWay;
	/**
	 * The direction in which the vehicle which emitted this position report drives. One of {@link #DIRECTION_EASTBOUND}
	 * and {@link #DIRECTION_WESTBOUND}.
	 */
	private Integer direction;
	/**
	 * Time (0. . .10799)^3 is a timestamp identifying the time at which the position report was emitted
	 */
	private long time;
	
	protected PosReport() {
		super();
	}
	
	/**
	 * Creates a {@code PosReport} with a creation timestamp taken from {@link StopWatch#getElapsedTime() } of the
	 * {@code timer} argument.
	 * 
	 * @param time
	 * @param vehicleIdentifier
	 * @param currentSpeed
	 * @param xWay
	 * @param lane
	 * @param segmentIdentifier
	 * @param direction
	 * @param position
	 * @param timer
	 */
	public PosReport(long time, Integer vehicleIdentifier, int currentSpeed, Integer xWay, Integer lane,
		Integer direction, SegmentIdentifier segmentIdentifier, Integer position, StopWatch timer) {
		super(LRBtuple.TYPE_POSITION_REPORT, System.currentTimeMillis(), timer);
		this.time = time;
		this.vehicleIdentifier = vehicleIdentifier;
		this.currentSpeed = currentSpeed;
		this.xWay = xWay;
		this.lane = lane;
		this.direction = direction;
		this.segmentIdentifier = segmentIdentifier;
		this.position = position;
	}
	
	public boolean isOnExitLane() {
		return this.getLane() == 4;
	}
	
	public Integer getDirection() {
		return direction;
	}
	
	public Integer getxWay() {
		return xWay;
	}
	
	/**
	 * @return the segmentIdentifier
	 */
	public SegmentIdentifier getSegmentIdentifier() {
		return segmentIdentifier;
	}
	
	/**
	 * @param segmentIdentifier
	 *            the segmentIdentifier to set
	 */
	public void setSegmentIdentifier(SegmentIdentifier segmentIdentifier) {
		this.segmentIdentifier = segmentIdentifier;
	}
	
	/**
	 * @return the position
	 */
	public Integer getPosition() {
		return position;
	}
	
	/**
	 * @param position
	 *            the position to set
	 */
	public void setPosition(Integer position) {
		this.position = position;
	}
	
	/**
	 * @return the currentSpeed
	 */
	public int getCurrentSpeed() {
		return currentSpeed;
	}
	
	/**
	 * @param currentSpeed
	 *            the currentSpeed to set
	 */
	public void setCurrentSpeed(int currentSpeed) {
		this.currentSpeed = currentSpeed;
	}
	
	/**
	 * @return the lane
	 */
	public Integer getLane() {
		return lane;
	}
	
	/**
	 * @param lane
	 *            the lane to set
	 */
	public void setLane(Integer lane) {
		this.lane = lane;
	}
	
	public Integer getVehicleIdentifier() {
		return vehicleIdentifier;
	}
	
	public long getTime() {
		return time;
	}
	
	protected void setTime(long time) {
		this.time = time;
	}
	
	@Override
	public boolean equals(Object obj) {
		if(obj == null) {
			return false;
		}
		if(getClass() != obj.getClass()) {
			return false;
		}
		final PosReport other = (PosReport)obj;
		if(this.segmentIdentifier != other.segmentIdentifier
			&& (this.segmentIdentifier == null || !this.segmentIdentifier.equals(other.segmentIdentifier))) {
			return false;
		}
		if(this.xWay != other.xWay && (this.xWay == null || !this.xWay.equals(other.xWay))) {
			return false;
		}
		if(this.direction != other.direction && (this.direction == null || !this.direction.equals(other.direction))) {
			return false;
		}
		return true;
	}
	
	@Override
	public int hashCode() {
		int hash = 3;
		hash = 89 * hash + (this.segmentIdentifier != null ? this.segmentIdentifier.hashCode() : 0);
		hash = 89 * hash + (this.xWay != null ? this.xWay.hashCode() : 0);
		hash = 89 * hash + (this.direction != null ? this.direction.hashCode() : 0);
		return hash;
	}
}
