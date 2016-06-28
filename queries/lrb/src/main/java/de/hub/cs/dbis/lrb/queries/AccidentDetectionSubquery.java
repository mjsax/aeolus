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
import de.hub.cs.dbis.lrb.operators.AccidentDetectionBolt;
import de.hub.cs.dbis.lrb.queries.utils.TopologyControl;
import de.hub.cs.dbis.lrb.types.internal.StoppedCarTuple;
import de.hub.cs.dbis.lrb.types.util.PositionIdentifier;





/**
 * {@link AccidentDetectionSubquery} assembles the "Accident Detection" subquery that must detect accidents.
 * 
 * @author mjsax
 */
public class AccidentDetectionSubquery extends AbstractQuery {
	
	public static void main(String[] args) throws IOException, InvalidTopologyException, AlreadyAliveException {
		new AccidentDetectionSubquery().parseArgumentsAndRun(args, new String[] {"accidentsOutput"});
	}
	
	/**
	 * {@inheritDoc}
	 * 
	 * Optional parameter {@code intermediateOutputs} specifies the output of {@link StoppedCarsSubquery}.
	 */
	@Override
	protected void addBolts(TopologyBuilder builder, String[] outputs, String[] intermediateOutputs) {
		new StoppedCarsSubquery().addBolts(builder, intermediateOutputs);
		
		try {
			builder
				.setBolt(TopologyControl.ACCIDENT_DETECTION_BOLT_NAME,
					new TimestampMerger(new AccidentDetectionBolt(), StoppedCarTuple.TIME_IDX),
					OperatorParallelism.get(TopologyControl.ACCIDENT_DETECTION_BOLT_NAME))
				.fieldsGrouping(TopologyControl.STOPPED_CARS_BOLT_NAME, PositionIdentifier.getSchema())
				.allGrouping(TopologyControl.STOPPED_CARS_BOLT_NAME, TimestampMerger.FLUSH_STREAM_ID);
		} catch(IllegalArgumentException e) {
			// happens if complete LRB is assembled, because "accident detection" is part of "accident query" and
			// "toll query"
			if(e.getMessage().equals(
				"Bolt has already been declared for id " + TopologyControl.ACCIDENT_DETECTION_BOLT_NAME)) {
				/* ignore */
			} else {
				throw e;
			}
		}
		
		if(outputs != null && outputs.length > 0) {
			if(outputs.length > 1) {
				System.err.println("WARN: <outputs>.length > 1 => partly ignored");
			}
			if(outputs[0] == null) {
				throw new IllegalArgumentException("Parameter <outputs>[0] must not be null.");
			}
			builder.setBolt("acc-sink", new FileFlushSinkBolt(outputs[0])).localOrShuffleGrouping(
				TopologyControl.ACCIDENT_DETECTION_BOLT_NAME, TopologyControl.ACCIDENTS_STREAM_ID);
		}
	}
}
