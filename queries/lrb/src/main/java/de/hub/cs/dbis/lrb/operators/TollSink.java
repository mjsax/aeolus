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
package de.hub.cs.dbis.lrb.operators;

import backtype.storm.tuple.Tuple;
import de.hub.cs.dbis.aeolus.sinks.FileSinkBolt;
import de.hub.cs.dbis.aeolus.utils.TimestampMerger;
import de.hub.cs.dbis.lrb.types.TollNotification;





/**
 * Write the {@link TollNotification} record to the result file: TYPE, VID, TIME, EMIT, SPEED, TOLL<br />
 * (VID must occur after TYPE for LRB validator tool)
 * 
 * @author mjsax
 */
public class TollSink extends FileSinkBolt {
	private static final long serialVersionUID = -6179260121297096162L;
	
	/** Internally (re)used object to access individual attributes. */
	final private TollNotification toll = new TollNotification();
	
	
	
	public TollSink(String filename) {
		super(filename);
		
	}
	
	
	
	@Override
	public void execute(Tuple input) {
		if(input.getSourceStreamId().equals(TimestampMerger.FLUSH_STREAM_ID)) {
			super.cleanup();
			return;
		}
		super.execute(input);
	}
	
	@Override
	public String tupleToString(Tuple input) {
		this.toll.clear();
		this.toll.addAll(input.getValues());
		
		return new StringBuffer().append(this.toll.getType()).append(',').append(this.toll.getVid()).append(',')
			.append(this.toll.getTime()).append(',').append(this.toll.getEmit()).append(',')
			.append(this.toll.getSpeed()).append(',').append(this.toll.getToll()).append('\n').toString();
	}
	
}
