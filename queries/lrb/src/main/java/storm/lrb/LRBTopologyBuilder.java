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
package storm.lrb;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.lrb.bolt.AccidentDetectionBolt;
import storm.lrb.bolt.AccidentNotificationBolt;
import storm.lrb.bolt.AccountBalanceBolt;
import de.hub.cs.dbis.lrb.operators.AverageSpeedBolt;
import storm.lrb.bolt.DailyExpenditureBolt;
import storm.lrb.bolt.DispatcherBolt;
import de.hub.cs.dbis.lrb.operators.TollNotificationBolt;
import storm.lrb.tools.StopWatch;
import backtype.storm.Config;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import de.hub.cs.dbis.lrb.toll.MemoryTollDataStore;
import de.hub.cs.dbis.lrb.types.AvgSpeedTuple;
import de.hub.cs.dbis.lrb.types.AvgVehicleSpeedTuple;
import de.hub.cs.dbis.lrb.types.LavTuple;
import de.hub.cs.dbis.lrb.types.PositionReport;
import de.hub.cs.dbis.lrb.types.SegmentIdentifier;
import java.util.Arrays;
import java.util.LinkedList;
import storm.lrb.model.Accident;
import storm.lrb.model.AccidentImmutable;
import storm.lrb.model.AccountBalanceRequest;
import storm.lrb.model.DailyExpenditureRequest;
import storm.lrb.model.TravelTimeRequest;
import storm.lrb.model.VehicleInfo;
import storm.lrb.tools.Helper;





/**
 * Builds the complete LRB topology and allows certain parameterization (fast switches of the spout and output, etc.).
 * Use it as you'd use {@link TopologyBuilder}, i.e. call {@link TopologyBuilder#createTopology() } after creating an
 * instance.
 * 
 * @author richter
 */
public class LRBTopologyBuilder extends TopologyBuilder {
	private final static Logger LOGGER = LoggerFactory.getLogger(LRBTopologyBuilder.class);
	private static final long serialVersionUID = 1L;
	private final String tollDataStoreClass = MemoryTollDataStore.class.getName();
	
	/**
	 * performs the assembly of the topology which can be retrieved with {@link #getStormTopology() }.
	 * 
	 * @param xways
	 * @param workers
	 * @param tasks
	 * @param executors
	 * @param offset
	 * @param spout
	 * @param stormConfig
	 *            an existing {@link Config} object to be filled with serialization and custom bolt information
	 * @param tollFileWriterBolt
	 *            an instance of {@link IRichBolt} to retrieve toll notifications output
	 * @param accidentOutputBolt
	 *            an instance of {@link IRichBolt} to retrieve accident output
	 * @param accountBalanceOutputBolt
	 *            an instance of {@link IRichBolt} to retrieve account balance output
	 * @param dailyExpenditureOutputBolt
	 *            an instance of {@link IRichBolt} to retrieve daily expenditure output
	 */
	public LRBTopologyBuilder(int xways, int workers, int tasks, int executors, int offset, IRichSpout spout,
		Config stormConfig, IRichBolt tollFileWriterBolt, IRichBolt accidentOutputBolt,
		IRichBolt accountBalanceOutputBolt, IRichBolt dailyExpenditureOutputBolt) {
		if(xways < 1) {
			throw new IllegalArgumentException(String.format("xways has to be >= 1, but is %d", xways));
		}
		if(workers < 1) {
			throw new IllegalArgumentException(String.format("workers has to be >= 1, but is %d", workers));
		}
		if(executors < 1) {
			throw new IllegalArgumentException(String.format("executors has to be >= 1, but is %d", executors));
		}
		if(tasks < 1) {
			throw new IllegalArgumentException(String.format("tasks has to be >= 1, but is %d", tasks));
		}
		if(offset < 0) {
			throw new IllegalArgumentException(String.format("offset has to be >= 0, but is %d", offset));
		}
		List<String> fields = new LinkedList<String>(Arrays.asList(TopologyControl.XWAY_FIELD_NAME,
			TopologyControl.DIRECTION_FIELD_NAME));
		StopWatch stormTimer = new StopWatch(offset);
		
		this.setSpout(TopologyControl.START_SPOUT_NAME, spout, 1);
		
		this.setBolt(TopologyControl.SPLIT_STREAM_BOLT_NAME, new DispatcherBolt(), 2 * xways).setNumTasks(4 * xways)
			.shuffleGrouping(TopologyControl.START_SPOUT_NAME, TopologyControl.SPOUT_STREAM_ID);// .allGrouping("Spout",
																								// "stormtimer");
		
		this.setBolt(TopologyControl.AVERAGE_SPEED_BOLT_NAME, new AverageSpeedBolt(), xways * 3).fieldsGrouping(
			TopologyControl.SPLIT_STREAM_BOLT_NAME,
			TopologyControl.POS_REPORTS_STREAM_ID,
			new Fields(TopologyControl.XWAY_FIELD_NAME, TopologyControl.SEGMENT_FIELD_NAME,
				TopologyControl.DIRECTION_FIELD_NAME));
		
		
		this.setBolt(TopologyControl.TOLL_FILE_WRITER_BOLT_NAME, tollFileWriterBolt, 1).allGrouping(
			TopologyControl.TOLL_NOTIFICATION_BOLT_NAME, TopologyControl.TOLL_NOTIFICATION_STREAM_ID);
		
		// this.setBolt("lavBolt", new SegmentStatsBolt(0), cmd.xways*3)
		// .fieldsGrouping("SplitStreamBolt", "PosReports", new Fields("xsd"));
		this.setBolt(TopologyControl.TOLL_NOTIFICATION_BOLT_NAME, new TollNotificationBolt(stormTimer), executors)
			.setNumTasks(tasks)
			.fieldsGrouping(TopologyControl.AVERAGE_SPEED_BOLT_NAME, Utils.DEFAULT_STREAM_ID, new Fields(fields))
			.fieldsGrouping(
				TopologyControl.ACCIDENT_DETECTION_BOLT_NAME,
				TopologyControl.ACCIDENT_INFO_STREAM_ID,
				new Fields(TopologyControl.POS_REPORT_FIELD_NAME, TopologyControl.SEGMENT_FIELD_NAME,
					TopologyControl.ACCIDENT_INFO_FIELD_NAME))
			.fieldsGrouping(TopologyControl.SPLIT_STREAM_BOLT_NAME, TopologyControl.POS_REPORTS_STREAM_ID,
				new Fields(fields));
		
		this.setBolt(TopologyControl.ACCIDENT_DETECTION_BOLT_NAME, new AccidentDetectionBolt(), xways).fieldsGrouping(
			TopologyControl.SPLIT_STREAM_BOLT_NAME, TopologyControl.POS_REPORTS_STREAM_ID,
			new Fields(TopologyControl.XWAY_FIELD_NAME, TopologyControl.DIRECTION_FIELD_NAME));
		
		this.setBolt(TopologyControl.ACCIDENT_NOTIFICATION_BOLT_NAME, new AccidentNotificationBolt(), xways)
			.fieldsGrouping(TopologyControl.ACCIDENT_DETECTION_BOLT_NAME, TopologyControl.ACCIDENT_INFO_STREAM_ID, // streamId
				new Fields(TopologyControl.POS_REPORT_FIELD_NAME))
			.fieldsGrouping(TopologyControl.SPLIT_STREAM_BOLT_NAME, TopologyControl.POS_REPORTS_STREAM_ID, // streamId
				new Fields(TopologyControl.XWAY_FIELD_NAME, TopologyControl.DIRECTION_FIELD_NAME));
		
		this.setBolt(TopologyControl.ACCIDENT_FILE_WRITER_BOLT_NAME, accidentOutputBolt, 1).allGrouping(
			TopologyControl.ACCIDENT_DETECTION_BOLT_NAME, TopologyControl.ACCIDENT_INFO_STREAM_ID);
		
		this.setBolt(TopologyControl.ACCOUNT_BALANCE_BOLT_NAME, new AccountBalanceBolt(), xways)
			.fieldsGrouping(TopologyControl.SPLIT_STREAM_BOLT_NAME, TopologyControl.ACCOUNT_BALANCE_REQUESTS_STREAM_ID,
				new Fields(TopologyControl.VEHICLE_ID_FIELD_NAME))
			.fieldsGrouping(TopologyControl.TOLL_NOTIFICATION_BOLT_NAME, TopologyControl.TOLL_NOTIFICATION_STREAM_ID,
				TollNotificationBolt.FIELDS_OUTGOING_TOLL_NOTIFICATION);
		
		this.setBolt(TopologyControl.ACCOUNT_BALANCE_FILE_WRITER_BOLT_NAME, accountBalanceOutputBolt, 1).allGrouping(
			TopologyControl.ACCOUNT_BALANCE_BOLT_NAME);
		
		this.setBolt(TopologyControl.DAILY_EXPEDITURE_BOLT_NAME, new DailyExpenditureBolt(), xways * 1)
			.shuffleGrouping(TopologyControl.SPLIT_STREAM_BOLT_NAME,
				TopologyControl.DAILY_EXPEDITURE_REQUESTS_STREAM_ID);
		
		this.setBolt(TopologyControl.DAILY_EXPEDITURE_FILE_WRITER_BOLT_NAME, dailyExpenditureOutputBolt, 1)
			.allGrouping(TopologyControl.DAILY_EXPEDITURE_BOLT_NAME);
		
		stormConfig.registerSerialization(AvgSpeedTuple.class);
		stormConfig.registerSerialization(AvgVehicleSpeedTuple.class);
		stormConfig.registerSerialization(LavTuple.class);
		stormConfig.registerSerialization(PositionReport.class);
		stormConfig.registerSerialization(SegmentIdentifier.class);
		stormConfig.registerSerialization(AccountBalanceRequest.class);
		stormConfig.registerSerialization(DailyExpenditureRequest.class);
		stormConfig.registerSerialization(TravelTimeRequest.class);
		stormConfig.registerSerialization(Accident.class);
		stormConfig.registerSerialization(VehicleInfo.class);
		stormConfig.registerSerialization(StopWatch.class);
		stormConfig.registerSerialization(AccidentImmutable.class);
		stormConfig.put(Helper.TOLL_DATA_STORE_CONF_KEY, tollDataStoreClass);
		
	}
	
}
