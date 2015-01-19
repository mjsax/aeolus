package storm.lrb.model;

/*
 * #%L
 * lrb
 * $Id:$
 * $HeadURL:$
 * %%
 * Copyright (C) 2014 - 2015 Humboldt-Universität zu Berlin
 * %%
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
 * #L%
 */

import java.io.Serializable;
import java.util.HashSet;

import org.apache.log4j.Logger;

/**
 * Immutable version of the Accident object for serialization.
 */
public class AccidentImmutable implements Serializable {
	
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = Logger.getLogger(AccidentImmutable.class);
	private int startTime;
	private int startMinute;
	private int lastUpdateTime;
	private int position;
	private boolean over = false;
	private HashSet<String> involvedSegs = new HashSet<String>();
	private HashSet<Integer> involvedCars = new HashSet<Integer>();
	private int maxPos;
	private int minPos;
	
	
	public AccidentImmutable() {

	}
	public AccidentImmutable(Accident accident) {
		startTime = accident.getStartTime();
		startMinute = Time.getMinute(startTime);
		position = accident.getAccidentPosition();
		lastUpdateTime = accident.getLastUpdateTime();
		involvedSegs = accident.getInvolvedSegs();
		involvedCars = accident.getInvolvedCars();
		over = accident.isOver();
	}

	
	public boolean active(int minute) {
		if(isOver())
			return minute <= Time.getMinute(lastUpdateTime);
		else
			return minute > startMinute;
	}

	
	
	public HashSet<Integer> getInvolvedCars(){
		return involvedCars;
	}
	
	public int getAccidentPosition(){
		return position;
	}
	public String getAccNotification(PosReport pos) {
	
		String notification = "1,"+pos.getTime()+","+ pos.getEmitTime()/1000+","
							+pos.getVidAsString() +","+((int)position/5280)
							+"***"+pos.getTime()+","+pos.getProcessingTime() 
							+ "###"+pos.toString()+"###";
		
		if(pos.getProcessingTimeSec()>5){
			LOG.error("Time Requirement not met: "+pos.getProcessingTimeSec()+ " for "+pos+"\n"+notification);
			if(LOG.isDebugEnabled())
				throw new Error("Time Requirement not met:"+pos+"\n"+notification);
		}
		return notification;
		
	}
	
	public final boolean isOver() {
		return over;
	}
	@Override
	public String toString() {
		return "Accident [startTime=" + startTime + ", startMinute="
				+ startMinute 
				+ ", lastUpdateTime=" + lastUpdateTime + ", position="
				+ position + ", over=" + over + ", involvedSegs="
				+ involvedSegs + ", involvedCars=" + involvedCars + ", maxPos="
				+ maxPos + ", minPos=" + minPos + "]";
	}
	
	
}
