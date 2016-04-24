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
package storm.lrb.tools;

import java.io.Serializable;

import backtype.storm.utils.Time;





public class StopWatch implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	private long startTime = 0;
	private long stopTime = 0;
	/**
	 * 
	 * offset in seconds
	 */
	private long offset = 0;
	private boolean running = false;
	
	/**
	 * This constructor takes an offset as a parameter and starts the timer
	 * 
	 * @param time
	 */
	public StopWatch(long time) {
		this.offset = time;
		this.start0();
	}
	
	public StopWatch() {}
	
	private void start0() {
		this.start();
	}
	
	public long getStartTime() {
		return this.startTime;
	}
	
	public long getStopTime() {
		return this.stopTime;
	}
	
	public long getOffset() {
		return this.offset;
	}
	
	/**
	 * starts the timer, previous starttime is overwritten
	 */
	public void start() {
		this.startTime = System.currentTimeMillis();// Time.currentTimeMillis();
		this.running = true;
	}
	
	/**
	 * starts the timer with offset, previous starttime is overwritten
	 * 
	 * @param offset
	 */
	public void start(long offset) {
		this.startTime = System.currentTimeMillis();
		this.running = true;
		this.offset = offset;
	}
	
	/**
	 * stops the timer, sets running to false
	 */
	public void stop() {
		this.stopTime = Time.currentTimeMillis();
		this.running = false;
	}
	
	/**
	 * get current stopwatch time ms (takes offsets into account)
	 * 
	 * @return now-startTime plus offset if the timer is running, stopTime-startTime plus offset else
	 */
	public long getElapsedTime() {
		return this.getDurationTime() + this.offset * 1000;
	}
	
	/**
	 * get current stopwatch time in seconds (takes offsets into account)
	 * 
	 * @return now-startTime if the timer is running, stopTime-startTime else - plus offset
	 */
	public long getElapsedTimeSecs() {
		return this.getDurationTimeSecs() + this.offset;
	}
	
	/**
	 * get the duration of the timer running in ms
	 * 
	 * @return now-startTime if the timer is running, stopTime-startTime else
	 */
	public long getDurationTime() {
		long elapsed;
		if(this.running) {
			elapsed = (Time.currentTimeMillis() - this.startTime);
		} else {
			elapsed = (this.stopTime - this.startTime);
		}
		return elapsed;
	}
	
	/**
	 * get the duration of the timer running in sec
	 * 
	 * @return now-startTime if the timer is running stopTime-startTime else
	 */
	public long getDurationTimeSecs() {
		
		return this.getDurationTime() / 1000;
	}
	
	public void setOffset(long time) {
		this.offset = time;
	}
	
	// time until next second is reached
	public long getTimeToFullSec() {
		
		return (this.getElapsedTimeSecs() + 1) * 1000 - this.getElapsedTime();
		
	}
	
	public boolean isRunning() {
		return this.running;
	}
	
	@Override
	public String toString() {
		return "StopWatch [offset=" + this.offset + ", running=" + this.running + "\n, getElapsedTime()="
			+ this.getElapsedTime() + ", getElapsedTimeSecs()=" + this.getElapsedTimeSecs() + ", getDurationTime()="
			+ this.getDurationTime() + ", getDurationTimeSecs()=" + this.getDurationTimeSecs() + "]";
	}
	
}
