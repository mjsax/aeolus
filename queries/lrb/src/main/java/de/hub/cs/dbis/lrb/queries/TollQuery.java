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
import java.util.Arrays;

import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
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
	
	public static void main(String[] args) throws IOException, InvalidTopologyException, AlreadyAliveException {
		new TollQuery().parseArgumentsAndRun(args, new String[] {"tollNotificationsOutput", "tollAssessmentsOutput"});
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
	protected void addBolts(TopologyBuilder builder, String[] outputs, String[] intermediateOutputs) {
		if(outputs == null) {
			throw new IllegalArgumentException("Parameter <outputs> must not be null.");
		}
		if(outputs.length < 2) {
			throw new IllegalArgumentException("Parameter <outputs> must provide two values.");
		}
		if(outputs[0] == null) {
			throw new IllegalArgumentException("Parameter <outputs>[0] must not be null.");
		}
		if(outputs[1] == null) {
			throw new IllegalArgumentException("Parameter <outputs>[1] must not be null.");
		}
		
		String[] subOutputAcc = null;
		String[] subIntermediateAcc = null;
		String[] subOutputLav = null;
		String[] subIntermediateLav = null;
		String[] subOutputCount = null;
		if(intermediateOutputs != null) {
			if(intermediateOutputs.length > 0) {
				subOutputAcc = new String[] {intermediateOutputs[0]};
			}
			if(intermediateOutputs.length > 1) {
				subIntermediateAcc = new String[] {intermediateOutputs[1]};
			}
			if(intermediateOutputs.length > 2) {
				subOutputLav = new String[] {intermediateOutputs[2]};
			}
			if(intermediateOutputs.length > 3) {
				subIntermediateLav = Arrays.copyOfRange(intermediateOutputs, 3, 5);
			}
			if(intermediateOutputs.length > 5) {
				subOutputCount = new String[] {intermediateOutputs[5]};
			}
			if(intermediateOutputs.length > 6) {
				System.err.println("WARN: <intermediateOutputs>.length > 6 => partly ignored");
			}
			
		}
		new AccidentDetectionSubquery().addBolts(builder, subOutputAcc, subIntermediateAcc);
		new LatestAverageVelocitySubquery().addBolts(builder, subOutputLav, subIntermediateLav);
		new CountVehicleSubquery().addBolts(builder, subOutputCount, null);
		
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
		
		if(outputs.length > 2) {
			System.err.println("WARN: <outputs>.length > 2 => partly ignored");
		}
		
		builder
			.setBolt(TopologyControl.TOLL_NOTIFICATIONS_FILE_WRITER_BOLT_NAME, new TollSink(outputs[0]),
				OperatorParallelism.get(TopologyControl.TOLL_NOTIFICATIONS_FILE_WRITER_BOLT_NAME))
			.localOrShuffleGrouping(TopologyControl.TOLL_NOTIFICATION_BOLT_NAME,
				TopologyControl.TOLL_NOTIFICATIONS_STREAM_ID)
			.allGrouping(TopologyControl.TOLL_NOTIFICATION_BOLT_NAME, TimestampMerger.FLUSH_STREAM_ID);
		
		builder
			.setBolt(TopologyControl.TOLL_ASSESSMENTS_FILE_WRITER_BOLT_NAME, new TollSink(outputs[1]),
				OperatorParallelism.get(TopologyControl.TOLL_ASSESSMENTS_FILE_WRITER_BOLT_NAME))
			.localOrShuffleGrouping(TopologyControl.TOLL_NOTIFICATION_BOLT_NAME,
				TopologyControl.TOLL_ASSESSMENTS_STREAM_ID)
			.allGrouping(TopologyControl.TOLL_NOTIFICATION_BOLT_NAME, TimestampMerger.FLUSH_STREAM_ID);
	}
	
}
