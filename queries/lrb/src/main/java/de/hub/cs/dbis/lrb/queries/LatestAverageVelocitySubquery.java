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

import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import de.hub.cs.dbis.aeolus.monitoring.MonitoringTopoloyBuilder;
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
	private final AverageSpeedSubquery avgSpdSubquery;
	private OptionSpec<String> output;
	
	
	
	public LatestAverageVelocitySubquery() {
		this(true);
	}
	
	public LatestAverageVelocitySubquery(boolean required) {
		this.avgSpdSubquery = new AverageSpeedSubquery(false);
		this.output = parser.accepts("lav-output", "Bolt local path to write latest average velocities.")
			.withRequiredArg().describedAs("file").ofType(String.class);
		
		if(required) {
			this.output = ((ArgumentAcceptingOptionSpec<String>)this.output).required();
		}
	}
	
	
	
	@Override
	protected void addBolts(MonitoringTopoloyBuilder builder, OptionSet options) {
		this.avgSpdSubquery.addBolts(builder, options);
		
		builder
			.setBolt(TopologyControl.LATEST_AVERAGE_SPEED_BOLT_NAME,
				new TimestampMerger(new LatestAverageVelocityBolt(), AvgSpeedTuple.MINUTE_IDX),
				OperatorParallelism.get(TopologyControl.LATEST_AVERAGE_SPEED_BOLT_NAME))
			.fieldsGrouping(TopologyControl.AVERAGE_SPEED_BOLT_NAME, SegmentIdentifier.getSchema())
			.allGrouping(TopologyControl.AVERAGE_SPEED_BOLT_NAME, TimestampMerger.FLUSH_STREAM_ID);
		
		if(options.has(this.output)) {
			builder.setSink("lav-sink", new FileFlushSinkBolt(options.valueOf(this.output))).localOrShuffleGrouping(
				TopologyControl.LATEST_AVERAGE_SPEED_BOLT_NAME, TopologyControl.LAVS_STREAM_ID);
		}
	}
	
	
	
	public static void main(String[] args) throws IOException, InvalidTopologyException, AlreadyAliveException {
		System.exit(new LatestAverageVelocitySubquery().parseArgumentsAndRun(args));
	}
	
}
