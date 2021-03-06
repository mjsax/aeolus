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
import de.hub.cs.dbis.lrb.types.util.ISegmentIdentifier;
import de.hub.cs.dbis.lrb.util.Time;





/**
 * {@link CountTuple} represents an intermediate result tuple; the number of vehicles in a segment within a 'minute
 * number' time frame (see {@link Time#getMinute(short)}).<br />
 * <br />
 * It has the following attributes: TIME, XWAY, SEGMENT, DIR, CNT
 * <ul>
 * <li>TIME: the 'minute number' (in LRB seconds) of the speed average</li>
 * <li>XWAY: the expressway the vehicle is on</li>
 * <li>SEGMENT: the segment number the vehicle is in</li>
 * <li>DIR: the vehicle's driving direction</li>
 * <li>CNT: the number of vehicles counted</li>
 * </ul>
 * 
 * @author mjsax
 */
public final class CountTuple extends Values implements ISegmentIdentifier {
	private static final long serialVersionUID = 2521804330216975272L;
	
	// attribute indexes
	/** The index of the TIME attribute. */
	public final static int TIME_IDX = 0;
	
	/** The index of the XWAY attribute. */
	public final static int XWAY_IDX = 1;
	
	/** The index of the SEGMENT attribute. */
	public final static int SEG_IDX = 2;
	
	/** The index of the DIR attribute. */
	public final static int DIR_IDX = 3;
	
	/** The index of the CNT attribute. */
	public final static int CNT_IDX = 4;
	
	
	
	public CountTuple() {}
	
	/**
	 * Instantiates a new {@link CountTuple} for the given attributes.
	 * 
	 * @param time
	 *            the 'minute number' (in LRB seconds) of the speed average
	 * @param xway
	 *            the expressway the vehicle is on
	 * @param segment
	 *            the segment number the vehicle is in
	 * @param diretion
	 *            the vehicle's driving direction
	 * @param count
	 *            the number the vehicles counted
	 */
	public CountTuple(Short time, Integer xway, Short segment, Short diretion, Integer count) {
		assert (time != null);
		assert (xway != null);
		assert (segment != null);
		assert (diretion != null);
		assert (count != null);
		
		super.add(TIME_IDX, time);
		super.add(XWAY_IDX, xway);
		super.add(SEG_IDX, segment);
		super.add(DIR_IDX, diretion);
		super.add(CNT_IDX, count);
		
		assert (super.size() == 5);
	}
	
	
	
	/**
	 * Returns the timestamp of this {@link CountTuple}.
	 * 
	 * @return the timestamp of this tuple
	 */
	public final Short getTime() {
		return (Short)super.get(TIME_IDX);
	}
	
	/**
	 * Returns the 'minute number' of this {@link CountTuple}.
	 * 
	 * @return the 'minute number' of this tuple
	 */
	public final short getMinuteNumber() {
		return Time.getMinute(this.getTime().shortValue());
	}
	
	/**
	 * Returns the expressway ID of this {@link CountTuple}.
	 * 
	 * @return the expressway of this tuple
	 */
	@Override
	public final Integer getXWay() {
		return (Integer)super.get(XWAY_IDX);
	}
	
	/**
	 * Returns the segment of this {@link CountTuple}.
	 * 
	 * @return the segment of this tuple
	 */
	@Override
	public final Short getSegment() {
		return (Short)super.get(SEG_IDX);
	}
	
	/**
	 * Returns the vehicle's direction of this {@link CountTuple}.
	 * 
	 * @return the direction of this tuple
	 */
	@Override
	public final Short getDirection() {
		return (Short)super.get(DIR_IDX);
	}
	
	/**
	 * Returns the number of vehicles of this {@link CountTuple}.
	 * 
	 * @return the count of this tuple
	 */
	public final Integer getCount() {
		return (Integer)super.get(CNT_IDX);
	}
	
	/**
	 * Returns the schema of a {@link CountTuple}..
	 * 
	 * @return the schema of a {@link CountTuple}
	 */
	public static Fields getSchema() {
		return new Fields(TopologyControl.MINUTE_FIELD_NAME, TopologyControl.XWAY_FIELD_NAME,
			TopologyControl.SEGMENT_FIELD_NAME, TopologyControl.DIRECTION_FIELD_NAME,
			TopologyControl.CAR_COUNT_FIELD_NAME);
	}
	
}
