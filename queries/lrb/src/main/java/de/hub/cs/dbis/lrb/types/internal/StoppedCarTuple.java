/*
 * #!
 * %
 * Copyright (C) 2014 - 2016 Humboldt-Universität zu Berlin
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
package de.hub.cs.dbis.lrb.types.internal;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import de.hub.cs.dbis.lrb.queries.utils.TopologyControl;
import de.hub.cs.dbis.lrb.types.util.IPositionIdentifier;
import de.hub.cs.dbis.lrb.util.Constants;
import de.hub.cs.dbis.lrb.util.Time;





/**
 * {@link StoppedCarTuple} represents an intermediate result tuple; it reports a stopped car for a specific time, xway,
 * lane, position, and direction. If VID is negative, it reports the first car movement for a stopped car.<br />
 * <br />
 * It has the following attributes: VID, MINUTE, XWAY, LANE, POSITON, DIR
 * <ul>
 * <li>VID: the vehicle id</li>
 * <li>TIME: the time when the vehicle stopped</li>
 * <li>XWAY: the expressway in which the vehicle stopped</li>
 * <li>LANE: the lane in which the vehicle stopped</li>
 * <li>POSITON: the position in which the vehicle stopped</li>
 * <li>DIR: the driving direction of the stopped vehicle</li>
 * </ul>
 * 
 * @author mjsax
 */
public final class StoppedCarTuple extends Values implements IPositionIdentifier {
	private static final long serialVersionUID = -7555340520758056252L;
	
	// attribute indexes
	/** The index of the VID attribute. */
	public final static int VID_IDX = 0;
	
	/** The index of the TIME attribute. */
	public final static int TIME_IDX = 1;
	
	/** The index of the XWAY attribute. */
	public final static int XWAY_IDX = 2;
	
	/** The index of the LANE attribute. */
	public final static int LANE_IDX = 3;
	
	/** The index of the POSITION attribute. */
	public final static int POS_IDX = 4;
	
	/** The index of the DIR attribute. */
	public final static int DIR_IDX = 5;
	
	
	
	public StoppedCarTuple() {}
	
	/**
	 * Instantiates a new {@link StoppedCarTuple} for the given attributes.
	 * 
	 * @param vid
	 *            the id of the stopped vehicle
	 * @param time
	 *            the time when the car stopped
	 * @param xway
	 *            the expressway the vehicle is on
	 * @param lane
	 *            the lane the vehicle is in
	 * @param position
	 *            the position of the vehicle
	 * @param direction
	 *            the vehicle's driving direction
	 */
	public StoppedCarTuple(Integer vid, Short time, Integer xway, Short lane, Integer position, Short direction) {
		assert (vid != null);
		assert (time != null);
		assert (xway != null);
		assert (lane != null);
		assert (position != null);
		assert (direction != null);
		
		super.add(VID_IDX, vid);
		super.add(TIME_IDX, time);
		super.add(XWAY_IDX, xway);
		super.add(LANE_IDX, lane);
		super.add(POS_IDX, position);
		super.add(DIR_IDX, direction);
		
		assert (super.size() == 6);
	}
	
	
	
	/**
	 * Returns the the vehicle ID of this {@link StoppedCarTuple}.
	 * 
	 * @return the VID of this tuple
	 */
	public final Integer getVid() {
		return (Integer)super.get(VID_IDX);
	}
	
	/**
	 * Returns the time of this {@link StoppedCarTuple}.
	 * 
	 * @return the time of this tuple
	 */
	public final Short getTime() {
		return (Short)super.get(TIME_IDX);
	}
	
	/**
	 * Returns the expressway ID of this {@link StoppedCarTuple}.
	 * 
	 * @return the expressway of this tuple
	 */
	@Override
	public final Integer getXWay() {
		return (Integer)super.get(XWAY_IDX);
	}
	
	/**
	 * Returns the lane of this {@link StoppedCarTuple}.
	 * 
	 * @return the lane of this tuple
	 */
	@Override
	public final Short getLane() {
		return (Short)super.get(LANE_IDX);
	}
	
	/**
	 * Returns the position of this {@link StoppedCarTuple}.
	 * 
	 * @return the position of this tuple
	 */
	@Override
	public final Integer getPosition() {
		return (Integer)super.get(POS_IDX);
	}
	
	/**
	 * Returns the segment of this {@link StoppedCarTuple}.
	 * 
	 * @return the segment of this tuple
	 */
	public final Short getSegment() {
		return new Short(
			(short)(((Integer)super.get(POS_IDX)).intValue() / (Constants.NUMBER_OF_POSITIONS / Constants.NUMBER_OF_SEGMENT)));
	}
	
	/**
	 * Returns the vehicle's direction of this {@link StoppedCarTuple}.
	 * 
	 * @return the direction of this tuple
	 */
	@Override
	public final Short getDirection() {
		return (Short)super.get(DIR_IDX);
	}
	
	/**
	 * Returns the 'minute number' of this {@link StoppedCarTuple}.
	 * 
	 * @return the 'minute number' of this {@link StoppedCarTuple}
	 */
	public final short getMinuteNumber() {
		return Time.getMinute(this.getTime().shortValue());
	}
	
	/**
	 * Returns the schema of a {@link StoppedCarTuple}.
	 * 
	 * @return the schema of a {@link StoppedCarTuple}
	 */
	public static Fields getSchema() {
		return new Fields(TopologyControl.VEHICLE_ID_FIELD_NAME, TopologyControl.TIMESTAMP_FIELD_NAME,
			TopologyControl.XWAY_FIELD_NAME, TopologyControl.LANE_FIELD_NAME, TopologyControl.POSITION_FIELD_NAME,
			TopologyControl.DIRECTION_FIELD_NAME);
	}
	
}
