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

//import com.citusdata.elven.linearRoad.FullSegment;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.lrb.bolt.SegmentIdentifier;





public class SegmentStatistics implements Serializable {
	
	private static final long serialVersionUID = 1L;
	// The number of minutes in the past for which we keep vehicle statistics.
	protected static final int MAX_HISTORICAL_MINS = 10;
	protected static final int TOTAL_MINS = 200;
	private final static Logger LOGGER = LoggerFactory.getLogger(SegmentStatistics.class);
	
	// We need to keep statistics for each segment. For this, we use a Map where
	// the key is a segment. The value is a list keeping time-based statistics.
	private final Map<SegmentIdentifier, List<MinuteStatistics>> segmentsMinutes = new HashMap<SegmentIdentifier, List<MinuteStatistics>>();
	
	private List<MinuteStatistics> minutesListGetAndInit(SegmentIdentifier segment, Map<SegmentIdentifier, List<MinuteStatistics>> segmentsMinutes) {
		
		List<MinuteStatistics> minutes = segmentsMinutes.get(segment);
		if(minutes == null) {
			minutes = new ArrayList<MinuteStatistics>(Collections.nCopies(TOTAL_MINS, (MinuteStatistics)null));
			segmentsMinutes.put(segment, minutes);
		}
		
		return minutes;
	}
	
	public int getSegmentCount() {
		return this.segmentsMinutes.size();
	}
	
	public Set<SegmentIdentifier> getXsdList() {
		return this.segmentsMinutes.keySet();
		
	}
	
	public void addVehicleSpeed(int minute, SegmentIdentifier xsd, int vid, int speed) {
		
		LOGGER.debug("segmentstats: %d, xsd: %d, speed: %d", minute, xsd, speed);
		// We need to keep segment statistics for each minute. For this, we use a
		// List where each index represents statistics for one minute.
		List<MinuteStatistics> minutes = this.minutesListGetAndInit(xsd, this.segmentsMinutes);
		
		MinuteStatistics minuteStatistics;
		synchronized(minutes) {
			minuteStatistics = minutes.get(minute);
			
			if(minuteStatistics == null) {
				minuteStatistics = new MinuteStatistics();
				minutes.set(minute, minuteStatistics);
				
				// Switching over to a new minute indicates that we may need to clear
				// historical statistics.
				int oldMinute = minute - MAX_HISTORICAL_MINS;
				if(oldMinute >= 0) {
					minutes.set(oldMinute, null);
				}
			}
		}
		
		minuteStatistics.addVehicleSpeed(vid, speed);
	}
	
	private MinuteStatistics findMinuteStatistics(int minute, SegmentIdentifier xsd) {
		List<MinuteStatistics> minutes = this.minutesListGetAndInit(xsd, this.segmentsMinutes);
		
		MinuteStatistics minuteStatistics;
		synchronized(minutes) {
			minuteStatistics = minutes.get(minute);
			if(minuteStatistics == null) {
				minuteStatistics = new MinuteStatistics();
				minutes.set(minute, minuteStatistics);
			}
		}
		
		return minuteStatistics;
	}
	
	/**
	 * 
	 * @param minute
	 * @param xsd
	 * @return
	 */
	/*
	 * internal implementation notes: - needs to be long because is unsed for list index access later
	 */
	public int vehicleCount(int minute, SegmentIdentifier xsd) {
		MinuteStatistics minuteStatistics = this.findMinuteStatistics(minute, xsd);
		return minuteStatistics.vehicleCount();
	}
	
	/**
	 * 
	 * @param minute
	 * @param xsd
	 * @return
	 */
	/*
	 * internal implementation notes: - needs to be long because is unsed for list index access later
	 */
	public double speedAverage(int minute, SegmentIdentifier xsd) {
		MinuteStatistics minuteStatistics = this.findMinuteStatistics(minute, xsd);
		return minuteStatistics.speedAverage();
	}
	
	@Override
	public String toString() {
		return "SegmentStatistics [segmentsMinutes=" + this.segmentsMinutes + "]";
	}
}
