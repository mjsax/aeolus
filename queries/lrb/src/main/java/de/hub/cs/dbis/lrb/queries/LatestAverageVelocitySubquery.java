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
import de.hub.cs.dbis.lrb.operators.LatestAverageVelocityBolt;
import de.hub.cs.dbis.lrb.queries.utils.TopologyControl;
import de.hub.cs.dbis.lrb.types.internal.AvgSpeedTuple;
import de.hub.cs.dbis.lrb.types.util.SegmentIdentifier;





/**
 * {@link LatestAverageVelocitySubquery} assembles the "Latest average velocity" subquery that computes the 5-minute
 * average speed per minute within each segment (ie, x-way, segment, and direction).
 * 
 * * @author mjsax
 */
public class LatestAverageVelocitySubquery extends AbstractQuery {
	
	public static void main(String[] args) throws IOException, InvalidTopologyException, AlreadyAliveException {
		new LatestAverageVelocitySubquery().parseArgumentsAndRun(args, new String[] {"lavOutput"});
	}
	
	/**
	 * {@inheritDoc}
	 * 
	 * Parameter {@code intermediateOutputs} specifies the output of {@link AverageSpeedSubquery} and
	 * {@link AverageVehicleSpeedSubquery}.
	 */
	@Override
	protected void addBolts(TopologyBuilder builder, String[] outputs, String[] intermediateOutputs) {
		String[] subOutput = null;
		String[] subIntermediate = null;
		if(intermediateOutputs != null && intermediateOutputs.length > 0) {
			subOutput = new String[] {intermediateOutputs[0]};
			if(intermediateOutputs.length > 1) {
				subIntermediate = new String[] {intermediateOutputs[1]};
				if(intermediateOutputs.length > 2) {
					System.err.println("WARN: <intermediateOutputs>.length > 2 => partly ignored");
				}
			}
		}
		new AverageSpeedSubquery().addBolts(builder, subOutput, subIntermediate);
		
		builder
			.setBolt(TopologyControl.LATEST_AVERAGE_SPEED_BOLT_NAME,
				new TimestampMerger(new LatestAverageVelocityBolt(), AvgSpeedTuple.MINUTE_IDX),
				OperatorParallelism.get(TopologyControl.LATEST_AVERAGE_SPEED_BOLT_NAME))
			.fieldsGrouping(TopologyControl.AVERAGE_SPEED_BOLT_NAME, SegmentIdentifier.getSchema())
			.allGrouping(TopologyControl.AVERAGE_SPEED_BOLT_NAME, TimestampMerger.FLUSH_STREAM_ID);
		
		if(outputs != null && outputs.length > 0) {
			if(outputs.length > 1) {
				System.err.println("WARN: <outputs>.length > 1 => partly ignored");
			}
			builder.setBolt("lav-sink", new FileFlushSinkBolt(outputs[0])).localOrShuffleGrouping(
				TopologyControl.LATEST_AVERAGE_SPEED_BOLT_NAME, TopologyControl.LAVS_STREAM_ID);
		}
	}
}
