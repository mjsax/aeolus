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
import de.hub.cs.dbis.lrb.operators.TollNotificationBolt;
import de.hub.cs.dbis.lrb.operators.TollSink;
import de.hub.cs.dbis.lrb.queries.utils.TollInputStreamsTsExtractor;
import de.hub.cs.dbis.lrb.queries.utils.TopologyControl;





/**
 * {@link TollQuery} assembles the "Toll Processing" query that must notify vehicles about toll to be paid within 5
 * seconds. Additionally, it assess the toll to be paid later on.
 * 
 * @author mjsax
 */
public class TollQuery extends AbstractQuery {
	private final AccidentDetectionSubquery accDetSubquery;
	private final LatestAverageVelocitySubquery lavSubquery;
	private final CountVehicleSubquery cntVehicleSubquery;
	private final OptionSpec<String> outputNot;
	private final OptionSpec<String> outputAss;
	
	
	
	public TollQuery() {
		this.accDetSubquery = new AccidentDetectionSubquery(false);
		this.lavSubquery = new LatestAverageVelocitySubquery(false);
		this.cntVehicleSubquery = new CountVehicleSubquery(false);
		
		this.outputNot = parser.accepts("toll-output", "Bolt local path to write toll notifications.")
			.withRequiredArg().describedAs("file").ofType(String.class).required();
		this.outputAss = parser.accepts("toll-ass-output", "Bolt local path to write toll assessments.")
			.withRequiredArg().describedAs("file").ofType(String.class).required();
	}
	
	
	
	/**
	 * {@inheritDoc}
	 * 
	 * Requires two specified outputs for "toll notification" and "toll assessments". Optional parameter
	 * {@code intermediateOutputs} specifies the output of {@link AccidentDetectionSubquery},
	 * {@link StoppedCarsSubquery}, {@link LatestAverageVelocitySubquery}, {@link AverageSpeedSubquery},
	 * {@link AverageVehicleSpeedSubquery}, and {@link CountVehicleSubquery}.
	 */
	@Override
	protected void addBolts(MonitoringTopoloyBuilder builder, OptionSet options) {
		this.accDetSubquery.addBolts(builder, options);
		this.lavSubquery.addBolts(builder, options);
		this.cntVehicleSubquery.addBolts(builder, options);
		
		builder
			.setBolt(TopologyControl.TOLL_NOTIFICATION_BOLT_NAME,
				new TimestampMerger(new TollNotificationBolt(), new TollInputStreamsTsExtractor()),
				OperatorParallelism.get(TopologyControl.TOLL_NOTIFICATION_BOLT_NAME))
			.fieldsGrouping(TopologyControl.SPLIT_STREAM_BOLT_NAME, TopologyControl.POSITION_REPORTS_STREAM_ID,
				new Fields(TopologyControl.VEHICLE_ID_FIELD_NAME))
			.allGrouping(TopologyControl.SPLIT_STREAM_BOLT_NAME, TimestampMerger.FLUSH_STREAM_ID)
			.allGrouping(TopologyControl.ACCIDENT_DETECTION_BOLT_NAME, TopologyControl.ACCIDENTS_STREAM_ID)
			.allGrouping(TopologyControl.ACCIDENT_DETECTION_BOLT_NAME, TimestampMerger.FLUSH_STREAM_ID)
			.allGrouping(TopologyControl.COUNT_VEHICLES_BOLT_NAME, TopologyControl.CAR_COUNTS_STREAM_ID)
			.allGrouping(TopologyControl.COUNT_VEHICLES_BOLT_NAME, TimestampMerger.FLUSH_STREAM_ID)
			.allGrouping(TopologyControl.LATEST_AVERAGE_SPEED_BOLT_NAME, TopologyControl.LAVS_STREAM_ID)
			.allGrouping(TopologyControl.LATEST_AVERAGE_SPEED_BOLT_NAME, TimestampMerger.FLUSH_STREAM_ID);
		
		builder
			.setSink(TopologyControl.TOLL_NOTIFICATIONS_FILE_WRITER_BOLT_NAME,
				new TollSink(options.valueOf(this.outputNot)),
				OperatorParallelism.get(TopologyControl.TOLL_NOTIFICATIONS_FILE_WRITER_BOLT_NAME))
			.localOrShuffleGrouping(TopologyControl.TOLL_NOTIFICATION_BOLT_NAME,
				TopologyControl.TOLL_NOTIFICATIONS_STREAM_ID)
			.allGrouping(TopologyControl.TOLL_NOTIFICATION_BOLT_NAME, TimestampMerger.FLUSH_STREAM_ID);
		
		builder
			.setSink(TopologyControl.TOLL_ASSESSMENTS_FILE_WRITER_BOLT_NAME,
				new TollSink(options.valueOf(this.outputAss)),
				OperatorParallelism.get(TopologyControl.TOLL_ASSESSMENTS_FILE_WRITER_BOLT_NAME))
			.localOrShuffleGrouping(TopologyControl.TOLL_NOTIFICATION_BOLT_NAME,
				TopologyControl.TOLL_ASSESSMENTS_STREAM_ID)
			.allGrouping(TopologyControl.TOLL_NOTIFICATION_BOLT_NAME, TimestampMerger.FLUSH_STREAM_ID);
	}
	
	
	
	public static void main(String[] args) throws Exception {
		System.exit(new TollQuery().parseArgumentsAndRun(args));
	}
	
}
