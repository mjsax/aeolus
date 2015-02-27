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
 * object to represent time travel requests
 */
/*
 * internal implementation notes: - does not implement clone because Values doesn't
 */
@SuppressWarnings("CloneableImplementsClone")
public class TTEstRequest extends LRBtuple implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	public TTEstRequest() {
		super();
		
	}
	
	public TTEstRequest(String tupel, StopWatch time) {
		super(tupel, time);
		
	}
	
	@Override
	public String toString() {
		return "TTimeReq [time=" + this.getTime() + ", vid=" + this.getVehicleIdentifier() + ", xway="
			+ this.getSegmentIdentifier().getxWay() + ", qid=" + this.getQueryIdentifier() + ", sInit="
			+ this.getSinit() + ", sEnd=" + this.getSend() + ", dow=" + this.getDow() + ", tod=" + this.getTod() + "]";
	}
	
}
