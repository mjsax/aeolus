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





/**
 * Immutable version of the Accident object for serialization.
 */
public class AccidentImmutable implements Serializable {
	
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(AccidentImmutable.class);
	private long startTime;
	private long startMinute;
	private long lastUpdateTime;
	private int position;
	private boolean over = false;
	private HashSet<SegmentIdentifier> involvedSegs = new HashSet<SegmentIdentifier>();
	private HashSet<Integer> involvedCars = new HashSet<Integer>();
	private int maxPos;
	private int minPos;
	
	public AccidentImmutable() {
		
	}
	
	public AccidentImmutable(Accident accident) {
		this.startTime = accident.getStartTime();
		this.startMinute = Time.getMinute(this.startTime);
		this.position = accident.getAccidentPosition();
		this.lastUpdateTime = accident.getLastUpdateTime();
		this.involvedSegs = accident.getInvolvedSegs();
		this.involvedCars = accident.getInvolvedCars();
		this.over = accident.isOver();
	}
	
	public boolean active(long minute) {
		if(this.isOver()) {
			return minute <= Time.getMinute(this.lastUpdateTime);
		} else {
			return minute > this.startMinute;
		}
	}
	
	public HashSet<Integer> getInvolvedCars() {
		return this.involvedCars;
	}
	
	public int getAccidentPosition() {
		return this.position;
	}
	
	public static void validatePositionReport(PosReport pos) {
		if(pos.getProcessingTimeSec() > 5) {
			throw new IllegalArgumentException("Time Requirement not met: " + pos.getProcessingTimeSec() + " for "
				+ pos);
		}
		
	}
	
	public boolean isOver() {
		return this.over;
	}
	
	@Override
	public String toString() {
		return "Accident [startTime=" + this.startTime + ", startMinute=" + this.startMinute + ", lastUpdateTime="
			+ this.lastUpdateTime + ", position=" + this.position + ", over=" + this.over + ", involvedSegs="
			+ this.involvedSegs + ", involvedCars=" + this.involvedCars + ", maxPos=" + this.maxPos + ", minPos="
			+ this.minPos + "]";
	}
	
}
