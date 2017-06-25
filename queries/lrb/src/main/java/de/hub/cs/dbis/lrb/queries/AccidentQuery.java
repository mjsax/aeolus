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

import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import backtype.storm.tuple.Fields;
import de.hub.cs.dbis.aeolus.monitoring.MonitoringTopoloyBuilder;
import de.hub.cs.dbis.aeolus.utils.TimestampMerger;
import de.hub.cs.dbis.lrb.operators.AccidentNotificationBolt;
import de.hub.cs.dbis.lrb.operators.AccidentSink;
import de.hub.cs.dbis.lrb.queries.utils.AccInputStreamsTsExtractor;
import de.hub.cs.dbis.lrb.queries.utils.TopologyControl;





/**
 * {@link AccidentQuery} assembles the "Accident Processing" query that must detect accidents and notify (close)
 * up-stream vehicles about accidents within 5 seconds.
 * 
 * @author mjsax
 */
public class AccidentQuery extends AbstractQuery {
	private final AccidentDetectionSubquery accDetectionSubquery;
	private final OptionSpec<String> output;
	
	
	
	public AccidentQuery() {
		this.accDetectionSubquery = new AccidentDetectionSubquery(false);
		this.output = parser.accepts("acc-output", "Bolt local path to write accident notifications.")
			.withRequiredArg().describedAs("file").ofType(String.class).required();
	}
	
	
	
	@Override
	protected void addBolts(MonitoringTopoloyBuilder builder, OptionSet options) {
		this.accDetectionSubquery.addBolts(builder, options);
		
		builder
			.setBolt(TopologyControl.ACCIDENT_NOTIFICATION_BOLT_NAME,
				new TimestampMerger(new AccidentNotificationBolt(), new AccInputStreamsTsExtractor()),
				OperatorParallelism.get(TopologyControl.ACCIDENT_NOTIFICATION_BOLT_NAME))
			.fieldsGrouping(TopologyControl.SPLIT_STREAM_BOLT_NAME, TopologyControl.POSITION_REPORTS_STREAM_ID,
				new Fields(TopologyControl.VEHICLE_ID_FIELD_NAME))
			.allGrouping(TopologyControl.SPLIT_STREAM_BOLT_NAME, TimestampMerger.FLUSH_STREAM_ID)
			.allGrouping(TopologyControl.ACCIDENT_DETECTION_BOLT_NAME, TopologyControl.ACCIDENTS_STREAM_ID)
			.allGrouping(TopologyControl.ACCIDENT_DETECTION_BOLT_NAME, TimestampMerger.FLUSH_STREAM_ID);
		
		builder
			.setSink(TopologyControl.ACCIDENT_FILE_WRITER_BOLT_NAME, new AccidentSink(options.valueOf(this.output)),
				OperatorParallelism.get(TopologyControl.ACCIDENT_FILE_WRITER_BOLT_NAME))
			.localOrShuffleGrouping(TopologyControl.ACCIDENT_NOTIFICATION_BOLT_NAME)
			.allGrouping(TopologyControl.ACCIDENT_NOTIFICATION_BOLT_NAME, TimestampMerger.FLUSH_STREAM_ID);
	}
	
	
	
	public static void main(String[] args) throws Exception {
		System.exit(new AccidentQuery().parseArgumentsAndRun(args));
	}
	
}
