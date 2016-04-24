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
package de.hub.cs.dbis.lrb.queries;

import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import de.hub.cs.dbis.aeolus.sinks.FileFlushSinkBolt;
import de.hub.cs.dbis.aeolus.utils.TimestampMerger;
import de.hub.cs.dbis.lrb.operators.CountVehiclesBolt;
import de.hub.cs.dbis.lrb.queries.utils.TopologyControl;
import de.hub.cs.dbis.lrb.types.PositionReport;
import de.hub.cs.dbis.lrb.types.util.SegmentIdentifier;





/**
 * {@link CountVehicleSubquery} assembles the "car count" subquery that computes number of cars per minute within each
 * segment (ie, x-way, segment, and direction). The car count must be a "count distinct", ie, if a car issue multiple
 * {@link PositionReport}s within a single segment, the car is only counted once.
 * 
 * * @author mjsax
 */
public class CountVehicleSubquery extends AbstractQuery {
	
	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
		new CountVehicleSubquery().parseArgumentsAndRun(args, new String[] {"vehicleCountsOutput"});
	}
	
	@Override
	protected void addBolts(TopologyBuilder builder, String[] outputs) {
		builder
			.setBolt(TopologyControl.COUNT_VEHICLES_BOLT_NAME,
				new TimestampMerger(new CountVehiclesBolt(), PositionReport.TIME_IDX))
			.fieldsGrouping(TopologyControl.SPLIT_STREAM_BOLT_NAME, TopologyControl.POSITION_REPORTS_STREAM_ID,
				SegmentIdentifier.getSchema())
			.allGrouping(TopologyControl.SPLIT_STREAM_BOLT_NAME, TimestampMerger.FLUSH_STREAM_ID);
		
		if(outputs != null) {
			if(outputs.length == 1) {
				builder.setBolt("sink", new FileFlushSinkBolt(outputs[0])).localOrShuffleGrouping(
					TopologyControl.COUNT_VEHICLES_BOLT_NAME, TopologyControl.CAR_COUNTS_STREAM_ID);
			} else {
				System.err.println("<outputs>.length != 1 => ignored");
			}
		}
	}
	
}
