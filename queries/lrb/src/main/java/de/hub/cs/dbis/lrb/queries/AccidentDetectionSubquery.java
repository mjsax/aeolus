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

import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import de.hub.cs.dbis.aeolus.monitoring.MonitoringTopoloyBuilder;
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
	private final StoppedCarsSubquery stoppedCarsSubquery;
	private OptionSpec<String> output;
	
	
	
	public AccidentDetectionSubquery() {
		this(true);
	}
	
	public AccidentDetectionSubquery(boolean required) {
		this.stoppedCarsSubquery = new StoppedCarsSubquery(false);
		this.output = parser.accepts("accidents-output", "Bolt local path to write detected accidents.")
			.withRequiredArg().describedAs("file").ofType(String.class);
		
		if(required) {
			this.output = ((ArgumentAcceptingOptionSpec<String>)this.output).required();
		}
	}
	
	
	
	@Override
	protected void addBolts(MonitoringTopoloyBuilder builder, OptionSet options) {
		this.stoppedCarsSubquery.addBolts(builder, options);
		
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
		
		if(options.has(this.output)) {
			builder.setSink("acc-sink", new FileFlushSinkBolt(options.valueOf(this.output))).localOrShuffleGrouping(
				TopologyControl.ACCIDENT_DETECTION_BOLT_NAME, TopologyControl.ACCIDENTS_STREAM_ID);
		}
	}
	
	
	
	public static void main(String[] args) throws Exception {
		System.exit(new AccidentDetectionSubquery().parseArgumentsAndRun(args));
	}
	
}
