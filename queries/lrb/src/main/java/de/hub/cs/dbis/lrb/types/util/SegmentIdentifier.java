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
package de.hub.cs.dbis.lrb.types.util;

import backtype.storm.tuple.Fields;
import de.hub.cs.dbis.lrb.queries.utils.TopologyControl;





/**
 * {@link SegmentIdentifier} represent an express way, segment, and direction.
 * 
 * @author richter
 * @author mjsax
 */
public class SegmentIdentifier implements ISegmentIdentifier {
	private static final long serialVersionUID = -1749126120064068807L;
	
	/** XWay (0...L−1) identifies the express way from which the position report is emitted. */
	private Integer xway;
	
	/** Seg (0...99) identifies the mile-long segment from which the position report is emitted. */
	private Short segment;
	
	/** Dir (0,1) indicates the direction (0 for Eastbound and 1 for Westbound). */
	private Short direction;
	
	
	
	public SegmentIdentifier() {}
	
	/**
	 * Instantiates a new {@link SegmentIdentifier}.
	 * 
	 * @param xWay
	 *            the xway of the segment
	 * @param segment
	 *            the segment id
	 * @param direction
	 *            the direction
	 */
	public SegmentIdentifier(Integer xWay, Short segment, Short direction) {
		assert (xWay != null);
		assert (segment != null);
		assert (direction != null);
		
		this.xway = xWay;
		this.segment = segment;
		this.direction = direction;
	}
	
	/**
	 * Instantiates a new {@link SegmentIdentifier}.
	 * 
	 * @param record
	 *            the tuple this segment ID is take from
	 */
	public SegmentIdentifier(ISegmentIdentifier record) {
		assert (record != null);
		
		this.xway = record.getXWay();
		this.segment = record.getSegment();
		this.direction = record.getDirection();
	}
	
	
	
	/**
	 * Returns the express way ID.
	 * 
	 * @return the express way ID
	 */
	@Override
	public Integer getXWay() {
		return this.xway;
	}
	
	/**
	 * Returns the segment number.
	 * 
	 * @return the segment number
	 */
	@Override
	public Short getSegment() {
		return this.segment;
	}
	
	/**
	 * Returns the direction.
	 * 
	 * @return the direction
	 */
	@Override
	public Short getDirection() {
		return this.direction;
	}
	
	/**
	 * Sets the express way ID.
	 */
	public void setXWay(Integer xway) {
		this.xway = xway;
	}
	
	/**
	 * Sets the segment number.
	 */
	public void setSegment(Short segment) {
		this.segment = segment;
	}
	
	/**
	 * Sets the direction.
	 */
	public void setDirection(Short direction) {
		this.direction = direction;
	}
	
	/**
	 * Set express way ID, segment number, and direction from the given record.
	 * 
	 * @param record
	 *            The record this {@link SegmentIdentifier} is populated from.
	 */
	public void set(ISegmentIdentifier record) {
		assert (record != null);
		
		this.xway = record.getXWay();
		this.segment = record.getSegment();
		this.direction = record.getDirection();
	}
	
	/**
	 * Return a copy of this {@link SegmentIdentifier}.
	 * 
	 * @return a copy of this {@link SegmentIdentifier}
	 */
	public SegmentIdentifier copy() {
		SegmentIdentifier sid = new SegmentIdentifier();
		sid.xway = this.xway;
		sid.segment = this.segment;
		sid.direction = this.direction;
		
		return sid;
	}
	
	/**
	 * Returns the schema of a {@link SegmentIdentifier}.
	 * 
	 * @return the schema of a {@link SegmentIdentifier}
	 */
	public static Fields getSchema() {
		return new Fields(TopologyControl.XWAY_FIELD_NAME, TopologyControl.SEGMENT_FIELD_NAME,
			TopologyControl.DIRECTION_FIELD_NAME);
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((this.direction == null) ? 0 : this.direction.hashCode());
		result = prime * result + ((this.segment == null) ? 0 : this.segment.hashCode());
		result = prime * result + ((this.xway == null) ? 0 : this.xway.hashCode());
		return result;
	}
	
	@Override
	public boolean equals(Object obj) {
		if(this == obj) {
			return true;
		}
		if(obj == null) {
			return false;
		}
		if(this.getClass() != obj.getClass()) {
			return false;
		}
		SegmentIdentifier other = (SegmentIdentifier)obj;
		if(this.direction == null) {
			if(other.direction != null) {
				return false;
			}
		} else if(!this.direction.equals(other.direction)) {
			return false;
		}
		if(this.segment == null) {
			if(other.segment != null) {
				return false;
			}
		} else if(!this.segment.equals(other.segment)) {
			return false;
		}
		if(this.xway == null) {
			if(other.xway != null) {
				return false;
			}
		} else if(!this.xway.equals(other.xway)) {
			return false;
		}
		return true;
	}
	
}
