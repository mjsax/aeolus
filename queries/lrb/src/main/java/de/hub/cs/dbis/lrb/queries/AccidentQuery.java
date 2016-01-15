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
import de.hub.cs.dbis.lrb.operators.AccidentNotificationBolt;
import de.hub.cs.dbis.lrb.operators.DispatcherBolt;
import de.hub.cs.dbis.lrb.operators.FileReaderSpout;
import de.hub.cs.dbis.lrb.types.PositionReport;
import de.hub.cs.dbis.lrb.types.internal.AccidentTuple;
import de.hub.cs.dbis.lrb.types.util.PositionIdentifier;
import de.hub.cs.dbis.lrb.util.Time;





/**
 * {@ AccidentQuery} assembles the "Accident Processing" query that must detect accidents and notify (close) up-stream
 * vehicles about accidents within 5 seconds.
 * 
 * @author mjsax
 */
public class AccidentQuery {
	
	public static StormTopology createTopology(String output) {
		TopologyBuilder builder = new TopologyBuilder();
		
		builder.setSpout(TopologyControl.SPOUT_NAME, new DataDrivenStreamRateDriverSpout<Long>(new FileReaderSpout(),
			0, TimeUnit.MILLISECONDS));
		
		builder.setBolt(TopologyControl.SPLIT_STREAM_BOLT_NAME, new TimestampMerger(new DispatcherBolt(), 0))
			.localOrShuffleGrouping(TopologyControl.SPOUT_NAME);
		
		builder.setBolt(TopologyControl.ACCIDENT_DETECTION_BOLT_NAME,
			new TimestampMerger(new AccidentDetectionBolt(), PositionReport.TIME_IDX)).fieldsGrouping(
			TopologyControl.SPLIT_STREAM_BOLT_NAME, TopologyControl.POSITION_REPORTS_STREAM_ID,
			PositionIdentifier.getSchema());
		
		builder
			.setBolt(TopologyControl.ACCIDENT_NOTIFICATION_BOLT_NAME,
				new TimestampMerger(new AccidentNotificationBolt(),
				// streams need to get merged by minute number
					new TimeStampExtractor<Tuple>() {
						private static final long serialVersionUID = -8976869891555736418L;
						
						@Override
						public long getTs(Tuple tuple) {
							if(tuple.getSourceStreamId().equals(TopologyControl.POSITION_REPORTS_STREAM_ID)) {
								return Time.getMinute(tuple.getShort(PositionReport.TIME_IDX).longValue());
							} else {
								return tuple.getShort(AccidentTuple.MINUTE_IDX).longValue();
							}
						}
					}))
			.fieldsGrouping(TopologyControl.SPLIT_STREAM_BOLT_NAME, TopologyControl.POSITION_REPORTS_STREAM_ID,
				new Fields(TopologyControl.VEHICLE_ID_FIELD_NAME))
			.allGrouping(TopologyControl.ACCIDENT_DETECTION_BOLT_NAME);
		
		builder.setBolt(TopologyControl.ACCIDENT_FILE_WRITER_BOLT_NAME, new FileFlushSinkBolt(output))
			.localOrShuffleGrouping(TopologyControl.ACCIDENT_NOTIFICATION_BOLT_NAME);
		
		return builder.createTopology();
	}
	
	public static void main(String[] args) throws Exception {
		if(args.length < 2) {
			showUsage();
		}
		
		if(args[0].equals("--local")) {
			if(args.length < 4) {
				showUsage();
			}
			
			Config c = new Config();
			c.put(FileReaderSpout.INPUT_FILE_NAME, args[1]);
			
			long runtime = 1000 * Long.parseLong(args[3]);
			
			LocalCluster lc = new LocalCluster();
			lc.submitTopology(TopologyControl.TOPOLOGY_NAME, c, AccidentQuery.createTopology(args[2]));
			
			Utils.sleep(runtime);
			
			lc.shutdown();
		} else {
			Config c = new Config();
			c.put(FileReaderSpout.INPUT_FILE_NAME, args[0]);
			c.put(Config.NIMBUS_HOST, "dbis71");
			
			StormSubmitter.submitTopology(TopologyControl.TOPOLOGY_NAME, c, AccidentQuery.createTopology(args[1]));
		}
	}
	
	private static void showUsage() {
		System.err.println("Missing arguments. Usage:");
		System.err.println("bin/storm jar jarfile.jar [--local] <input> <output> [<runtime>]");
		System.err.println("  <runtime> is only valid AND required if'--local' is specified");
		System.exit(-1);
	}
	
}
