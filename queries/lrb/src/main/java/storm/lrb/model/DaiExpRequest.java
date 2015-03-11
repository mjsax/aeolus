package storm.lrb.model;

/*
 * #%L
 * lrb
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

import storm.lrb.tools.StopWatch;





/**
 * Object to represent daily expnditure requests
 */
/*
 * internal implementation notes: - does not implement clone because Values doesn't
 */
@SuppressWarnings("CloneableImplementsClone")
public class DaiExpRequest extends LRBtuple implements Serializable {
	
	private static final long serialVersionUID = 1L;
	public static final int TYPE = 3;
	
	protected DaiExpRequest() {
		super();
		
	}
	
	public DaiExpRequest(String tupel, StopWatch time) {
		super(TYPE, tupel, time);
		
	}
	
	@Override
	public String toString() {
		return "ExpenditureReq [time=" + this.getTime() + ", vid=" + this.getVehicleIdentifier() + ", xway="
			+ this.getSegmentIdentifier().getxWay() + ", qid=" + this.getQueryIdentifier() + ", day=" + this.getDay()
			+ "]";
	}
	
}
