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

import java.io.IOException;

import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import de.hub.cs.dbis.aeolus.sinks.FileFlushSinkBolt;
import de.hub.cs.dbis.aeolus.utils.TimestampMerger;
import de.hub.cs.dbis.lrb.operators.AverageSpeedBolt;
import de.hub.cs.dbis.lrb.queries.utils.TopologyControl;
import de.hub.cs.dbis.lrb.types.internal.AvgVehicleSpeedTuple;
import de.hub.cs.dbis.lrb.types.util.SegmentIdentifier;





/**
 * {@link AverageSpeedSubquery} assembles the "Average Speed" subquery that computes the average speed per minute within
 * each segment (ie, x-way, segment, and direction).
 * 
 * * @author mjsax
 */
public class AverageSpeedSubquery extends AbstractQuery {
	
	public static void main(String[] args) throws IOException, InvalidTopologyException, AlreadyAliveException {
		new AverageSpeedSubquery().parseArgumentsAndRun(args, new String[] {"averageSpeedOutput"});
	}
	
	/**
	 * {@inheritDoc}
	 * 
	 * Optional parameter {@code intermediateOutputs} specifies the output of {@link AverageVehicleSpeedSubquery}.
	 */
	@Override
	protected void addBolts(TopologyBuilder builder, String[] outputs, String[] intermediateOutputs) {
		new AverageVehicleSpeedSubquery().addBolts(builder, intermediateOutputs);
		
		builder
			.setBolt(TopologyControl.AVERAGE_SPEED_BOLT_NAME,
				new TimestampMerger(new AverageSpeedBolt(), AvgVehicleSpeedTuple.MINUTE_IDX),
				OperatorParallelism.get(TopologyControl.AVERAGE_SPEED_BOLT_NAME))
			.fieldsGrouping(TopologyControl.AVERAGE_VEHICLE_SPEED_BOLT_NAME, SegmentIdentifier.getSchema())
			.allGrouping(TopologyControl.AVERAGE_VEHICLE_SPEED_BOLT_NAME, TimestampMerger.FLUSH_STREAM_ID);
		
		if(outputs != null && outputs.length > 0) {
			if(outputs.length > 1) {
				System.err.println("WARN: <outputs>.length > 1 => partly ignored");
			}
			builder.setBolt("avg-speed-sink", new FileFlushSinkBolt(outputs[0])).localOrShuffleGrouping(
				TopologyControl.AVERAGE_SPEED_BOLT_NAME);
		}
	}
	
}
