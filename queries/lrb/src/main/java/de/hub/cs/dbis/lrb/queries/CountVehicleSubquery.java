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
	private OptionSpec<String> output;
	
	
	
	public CountVehicleSubquery() {
		this(true);
	}
	
	public CountVehicleSubquery(boolean required) {
		this.output = parser.accepts("cnt-output", "Bolt local path to write car counts.").withRequiredArg()
			.describedAs("file").ofType(String.class);
		
		if(required) {
			this.output = ((ArgumentAcceptingOptionSpec<String>)this.output).required();
		}
	}
	
	
	
	@Override
	protected void addBolts(MonitoringTopoloyBuilder builder, OptionSet options) {
		builder
			.setBolt(TopologyControl.COUNT_VEHICLES_BOLT_NAME,
				new TimestampMerger(new CountVehiclesBolt(), PositionReport.TIME_IDX),
				OperatorParallelism.get(TopologyControl.COUNT_VEHICLES_BOLT_NAME))
			.fieldsGrouping(TopologyControl.SPLIT_STREAM_BOLT_NAME, TopologyControl.POSITION_REPORTS_STREAM_ID,
				SegmentIdentifier.getSchema())
			.allGrouping(TopologyControl.SPLIT_STREAM_BOLT_NAME, TimestampMerger.FLUSH_STREAM_ID);
		
		if(options.has(this.output)) {
			builder.setSink("cnt-sink", new FileFlushSinkBolt(options.valueOf(this.output))).localOrShuffleGrouping(
				TopologyControl.COUNT_VEHICLES_BOLT_NAME, TopologyControl.CAR_COUNTS_STREAM_ID);
		}
	}
	
	
	
	public static void main(String[] args) throws Exception {
		System.exit(new CountVehicleSubquery().parseArgumentsAndRun(args));
	}
	
}
