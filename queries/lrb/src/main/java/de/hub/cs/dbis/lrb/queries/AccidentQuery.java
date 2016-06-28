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
import backtype.storm.tuple.Fields;
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
	
	public static void main(String[] args) throws IOException, InvalidTopologyException, AlreadyAliveException {
		new AccidentQuery().parseArgumentsAndRun(args, new String[] {"accidentNotificationsOutput"});
	}
	
	/**
	 * {@inheritDoc}
	 * 
	 * Requires one specified output. Optional parameter {@code intermediateOutputs} specifies the output of
	 * {@link AccidentDetectionSubquery} and {@link StoppedCarsSubquery}.
	 */
	@Override
	protected void addBolts(TopologyBuilder builder, String[] outputs, String[] intermediateOutputs) {
		if(outputs == null) {
			throw new IllegalArgumentException("Parameter <outputs> must not be null.");
		}
		if(outputs.length == 0) {
			throw new IllegalArgumentException("Parameter <outputs> must not be empty.");
		}
		if(outputs[0] == null) {
			throw new IllegalArgumentException("Parameter <outputs>[0] must not be null.");
		}
		
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
		new AccidentDetectionSubquery().addBolts(builder, subOutput, subIntermediate);
		
		builder
			.setBolt(TopologyControl.ACCIDENT_NOTIFICATION_BOLT_NAME,
				new TimestampMerger(new AccidentNotificationBolt(), new AccInputStreamsTsExtractor()),
				OperatorParallelism.get(TopologyControl.ACCIDENT_NOTIFICATION_BOLT_NAME))
			.fieldsGrouping(TopologyControl.SPLIT_STREAM_BOLT_NAME, TopologyControl.POSITION_REPORTS_STREAM_ID,
				new Fields(TopologyControl.VEHICLE_ID_FIELD_NAME))
			.allGrouping(TopologyControl.SPLIT_STREAM_BOLT_NAME, TimestampMerger.FLUSH_STREAM_ID)
			.allGrouping(TopologyControl.ACCIDENT_DETECTION_BOLT_NAME, TopologyControl.ACCIDENTS_STREAM_ID)
			.allGrouping(TopologyControl.ACCIDENT_DETECTION_BOLT_NAME, TimestampMerger.FLUSH_STREAM_ID);
		
		if(outputs.length > 1) {
			System.err.println("WARN: <outputs>.length > 1 => partly ignored");
		}
		
		builder
			.setBolt(TopologyControl.ACCIDENT_FILE_WRITER_BOLT_NAME, new AccidentSink(outputs[0]),
				OperatorParallelism.get(TopologyControl.ACCIDENT_FILE_WRITER_BOLT_NAME))
			.localOrShuffleGrouping(TopologyControl.ACCIDENT_NOTIFICATION_BOLT_NAME)
			.allGrouping(TopologyControl.ACCIDENT_NOTIFICATION_BOLT_NAME, TimestampMerger.FLUSH_STREAM_ID);
	}
	
}
