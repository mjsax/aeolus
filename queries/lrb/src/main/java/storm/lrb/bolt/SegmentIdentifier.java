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
package storm.lrb.bolt;



/**
 * 
 * @author richter
 */
public class SegmentIdentifier {
	public static final int DIRECTION_EASTBOUND = 0;
	public static final int DIRECTION_WESTBOUND = 1;
	
	/**
	 * XWay (0. . . L−1) identifies the expressway from which the position report is emitted
	 */
	private Integer xway;
	/**
	 * Dir (0. . .1) indicates the direction (0 for Eastbound and 1 for Westbound)
	 */
	private Integer direction;
	/**
	 * Seg (0. . .99) identiﬁes the mile-long segment from which the position report is emitted
	 */
	private Integer segment;
	
	protected SegmentIdentifier() {}
	
	public SegmentIdentifier(int xWay, int segment, int direction) {
		this.xway = xWay;
		this.segment = segment;
		this.direction = direction;
	}
	
	/**
	 * @return the xWay
	 */
	public int getxWay() {
		return this.xway;
	}
	
	/**
	 * @param xWay
	 *            the xWay to set
	 */
	public void setxWay(int xWay) {
		this.xway = xWay;
	}
	
	/**
	 * @return the segment
	 */
	public int getSegment() {
		return this.segment;
	}
	
	/**
	 * @param segment
	 *            the segment to set
	 */
	public void setSegment(int segment) {
		this.segment = segment;
	}
	
	/**
	 * @return the direction
	 */
	public int getDirection() {
		return this.direction;
	}
	
	/**
	 * @param direction
	 *            the direction to set
	 */
	public void setDirection(int direction) {
		this.direction = direction;
	}
	
	@Override
	public int hashCode() {
		int hash = 5;
		hash = 67 * hash + (this.xway != null ? this.xway.hashCode() : 0);
		hash = 67 * hash + (this.direction != null ? this.direction.hashCode() : 0);
		hash = 67 * hash + (this.segment != null ? this.segment.hashCode() : 0);
		return hash;
	}
	
	@Override
	public boolean equals(Object obj) {
		if(obj == null) {
			return false;
		}
		if(this.getClass() != obj.getClass()) {
			return false;
		}
		final SegmentIdentifier other = (SegmentIdentifier)obj;
		if(this.getxWay() != other.getxWay() && (this.xway == null || !this.xway.equals(other.getxWay()))) {
			return false;
		}
		if(this.getDirection() != other.getDirection()
			&& (this.direction == null || !this.direction.equals(other.getDirection()))) {
			return false;
		}
		return !(this.getSegment() != other.getSegment() && (this.segment == null || !this.segment.equals(other
			.getSegment())));
	}
}
