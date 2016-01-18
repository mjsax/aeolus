/*
 * #!
 * %
 * Copyright (C) 2014 - 2015 Humboldt-Universität zu Berlin
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.lrb.TopologyControl;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;
import de.hub.cs.dbis.aeolus.sinks.FileFlushSinkBolt;
import de.hub.cs.dbis.aeolus.spouts.DataDrivenStreamRateDriverSpout;
import de.hub.cs.dbis.aeolus.spouts.DataDrivenStreamRateDriverSpout.TimeUnit;
import de.hub.cs.dbis.aeolus.utils.TimeStampExtractor;
import de.hub.cs.dbis.aeolus.utils.TimestampMerger;
import de.hub.cs.dbis.lrb.operators.AccidentDetectionBolt;
import de.hub.cs.dbis.lrb.operators.AverageSpeedBolt;
import de.hub.cs.dbis.lrb.operators.AverageVehicleSpeedBolt;
import de.hub.cs.dbis.lrb.operators.CountVehiclesBolt;
import de.hub.cs.dbis.lrb.operators.DispatcherBolt;
import de.hub.cs.dbis.lrb.operators.FileReaderSpout;
import de.hub.cs.dbis.lrb.operators.LatestAverageVelocityBolt;
import de.hub.cs.dbis.lrb.operators.TollNotificationBolt;
import de.hub.cs.dbis.lrb.types.PositionReport;
import de.hub.cs.dbis.lrb.types.internal.AccidentTuple;
import de.hub.cs.dbis.lrb.types.internal.AvgSpeedTuple;
import de.hub.cs.dbis.lrb.types.internal.AvgVehicleSpeedTuple;
import de.hub.cs.dbis.lrb.types.internal.CountTuple;
import de.hub.cs.dbis.lrb.types.internal.LavTuple;
import de.hub.cs.dbis.lrb.types.util.PositionIdentifier;
import de.hub.cs.dbis.lrb.types.util.SegmentIdentifier;
import de.hub.cs.dbis.lrb.util.Time;





/**
 * {@link TollQuery} assembles the "Toll Processing" query that must notify vehicles about toll to be paid within 5
 * seconds. Additionally, it assess the toll to be paid later on.
 * 
 * @author mjsax
 */
public class TollQuery {
	
	public static StormTopology createTopology(String notificationsOutput, String assessmentsOutput) {
		TopologyBuilder builder = new TopologyBuilder();
		
		// TODO TimeUnit.SECONDS
		builder.setSpout(TopologyControl.SPOUT_NAME, new DataDrivenStreamRateDriverSpout<Long>(new FileReaderSpout(),
			0, TimeUnit.MICROSECONDS));
		
		builder.setBolt(TopologyControl.SPLIT_STREAM_BOLT_NAME, new TimestampMerger(new DispatcherBolt(), 0))
			.localOrShuffleGrouping(TopologyControl.SPOUT_NAME);
		
		builder.setBolt(TopologyControl.ACCIDENT_DETECTION_BOLT_NAME,
			new TimestampMerger(new AccidentDetectionBolt(), PositionReport.TIME_IDX)).fieldsGrouping(
			TopologyControl.SPLIT_STREAM_BOLT_NAME, TopologyControl.POSITION_REPORTS_STREAM_ID,
			PositionIdentifier.getSchema());
		
		builder.setBolt(TopologyControl.COUNT_VEHICLES_BOLT_NAME,
			new TimestampMerger(new CountVehiclesBolt(), PositionReport.TIME_IDX)).fieldsGrouping(
			TopologyControl.SPLIT_STREAM_BOLT_NAME, TopologyControl.POSITION_REPORTS_STREAM_ID,
			SegmentIdentifier.getSchema());
		
		builder.setBolt(TopologyControl.AVERAGE_VEHICLE_SPEED_BOLT_NAME,
			new TimestampMerger(new AverageVehicleSpeedBolt(), PositionReport.TIME_IDX)).fieldsGrouping(
			TopologyControl.SPLIT_STREAM_BOLT_NAME, TopologyControl.POSITION_REPORTS_STREAM_ID,
			new Fields(TopologyControl.VEHICLE_ID_FIELD_NAME));
		
		builder.setBolt(TopologyControl.AVERAGE_SPEED_BOLT_NAME,
			new TimestampMerger(new AverageSpeedBolt(), AvgVehicleSpeedTuple.MINUTE_IDX)).fieldsGrouping(
			TopologyControl.AVERAGE_VEHICLE_SPEED_BOLT_NAME, SegmentIdentifier.getSchema());
		
		builder.setBolt(TopologyControl.LAST_AVERAGE_SPEED_BOLT_NAME,
			new TimestampMerger(new LatestAverageVelocityBolt(), AvgSpeedTuple.MINUTE_IDX)).fieldsGrouping(
			TopologyControl.AVERAGE_SPEED_BOLT_NAME, SegmentIdentifier.getSchema());
		
		builder
			.setBolt(TopologyControl.TOLL_NOTIFICATION_BOLT_NAME,
				new TimestampMerger(new TollNotificationBolt(), new TollInputStreamsMerger()))
			.fieldsGrouping(TopologyControl.SPLIT_STREAM_BOLT_NAME, TopologyControl.POSITION_REPORTS_STREAM_ID,
				new Fields(TopologyControl.VEHICLE_ID_FIELD_NAME))
			.allGrouping(TopologyControl.ACCIDENT_DETECTION_BOLT_NAME, TopologyControl.ACCIDENTS_STREAM_ID)
			.allGrouping(TopologyControl.COUNT_VEHICLES_BOLT_NAME, TopologyControl.CAR_COUNTS_STREAM_ID)
			.allGrouping(TopologyControl.LAST_AVERAGE_SPEED_BOLT_NAME, TopologyControl.LAVS_STREAM_ID);
		
		builder.setBolt(TopologyControl.TOLL_NOTIFICATIONS_FILE_WRITER_BOLT_NAME,
			new FileFlushSinkBolt(notificationsOutput)).localOrShuffleGrouping(
			TopologyControl.TOLL_NOTIFICATION_BOLT_NAME, TopologyControl.TOLL_NOTIFICATIONS_STREAM_ID);
		
		builder.setBolt(TopologyControl.TOLL_ASSESSMENTS_FILE_WRITER_BOLT_NAME,
			new FileFlushSinkBolt(assessmentsOutput)).localOrShuffleGrouping(
			TopologyControl.TOLL_NOTIFICATION_BOLT_NAME, TopologyControl.TOLL_ASSESSMENTS_STREAM_ID);
		
		return builder.createTopology();
	}
	
	public static void main(String[] args) throws Exception {
		if(args.length < 2) {
			showUsage();
		}
		
		if(args[0].equals("--local")) {
			if(args.length < 5) {
				showUsage();
			}
			
			Config c = new Config();
			c.put(FileReaderSpout.INPUT_FILE_NAME, args[1]);
			// c.setDebug(true);
			
			long runtime = 1000 * Long.parseLong(args[4]);
			
			LocalCluster lc = new LocalCluster();
			lc.submitTopology(TopologyControl.TOPOLOGY_NAME, c, TollQuery.createTopology(args[2], args[3]));
			
			Utils.sleep(runtime);
			
			lc.shutdown();
		} else {
			Config c = new Config();
			c.put(FileReaderSpout.INPUT_FILE_NAME, args[0]);
			
			StormSubmitter.submitTopology(TopologyControl.TOPOLOGY_NAME, c, TollQuery.createTopology(args[1], args[2]));
		}
	}
	
	private static void showUsage() {
		System.err.println("Missing arguments. Usage:");
		System.err
			.println("bin/storm jar jarfile.jar [--local] <input> <notificationsOutput> <assessmentsOutput> [<runtime>]");
		System.err.println("  <runtime> is only valid AND required if'--local' is specified");
		System.exit(-1);
	}
	
	private static final class TollInputStreamsMerger implements TimeStampExtractor<Tuple> {
		private static final long serialVersionUID = -234551807946550L;
		private static final Logger LOGGER = LoggerFactory.getLogger(TollInputStreamsMerger.class);
		
		@Override
		public long getTs(Tuple tuple) {
			final String inputStreamId = tuple.getSourceStreamId();
			if(inputStreamId.equals(TopologyControl.POSITION_REPORTS_STREAM_ID)) {
				// System.out.println("pos: " + Time.getMinute(tuple.getShort(PositionReport.TIME_IDX).longValue()));
				return Time.getMinute(tuple.getShort(PositionReport.TIME_IDX).longValue());
			} else if(inputStreamId.equals(TopologyControl.ACCIDENTS_STREAM_ID)) {
				// System.out.println("acc: " + tuple.getShort(AccidentTuple.MINUTE_IDX).longValue());
				return tuple.getShort(AccidentTuple.MINUTE_IDX).longValue();
			} else if(inputStreamId.equals(TopologyControl.CAR_COUNTS_STREAM_ID)) {
				// System.out.println("nct: " + tuple.getShort(CountTuple.MINUTE_IDX).longValue());
				return tuple.getShort(CountTuple.MINUTE_IDX).longValue();
			} else if(inputStreamId.equals(TopologyControl.LAVS_STREAM_ID)) {
				// System.out.println("lav: " + (tuple.getShort(LavTuple.MINUTE_IDX).longValue() - 1));
				return tuple.getShort(LavTuple.MINUTE_IDX).longValue() - 1;
			} else {
				LOGGER.error("Unknown input stream: '" + inputStreamId + "' for tuple " + tuple);
				throw new RuntimeException("Unknown input stream: '" + inputStreamId + "' for tuple " + tuple);
			}
		}
	}
	
	
}
