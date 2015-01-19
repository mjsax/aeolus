/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package debs2013;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;





/**
 * {@link ParseBolt} parses {@code rawTuple} and emits a single output for each successfully processed input tuple.
 * {@link ParseBolt} produces three output streams: (1) {@code playerData} contains all sensor readings from all players
 * of both teams; (2) {@code ballData} contains all sensors readings from all balls; (3) {@code refereeData} contains
 * all sensor readings from the referee.<br />
 * <br />
 * <strong>Input schema:</strong> {@code <ts:}{@link Long}{@code ,rawTuple:}{@link String}{@code >}<br />
 * Attribute {@code rawTuple} is expected to be in CSV-format ({@code <sid,ts,x,y,z,|v|,|a|,vx,vy,vz,ax,ay,az>}).<br />
 * <br />
 * <strong>Output schema:</strong> {@code <sid:}{@link Long}{@code ,ts:}{@link Long}{@code ,x:}{@link Integer}{@code ,y:}
 * {@link Integer}{@code ,z:}{@link Integer}{@code ,|v|:}{@link Integer}{@code ,|a|:}{@link Integer}{@code ,vx:}
 * {@link Integer}{@code ,vy:}{@link Integer}{@code ,vz:}{@link Integer}{@code ,ax:}{@link Integer}{@code ,ay:}
 * {@link Integer}{@code ,az:}{@link Integer}{@code >}<br />
 * Output attribute {@code ts} corresponds to the given input attribute {@code ts} (it is <em>not</em> extracted from
 * {@code rawTuple}).
 * 
 * @author Leonardo Aniello (Sapienza Università di Roma, Roma, Italy)
 * @author Roberto Baldoni (Sapienza Università di Roma, Roma, Italy)
 * @author Leonardo Querzoni (Sapienza Università di Roma, Roma, Italy)
 * @author Matthias J. Sax
 */
public class ParseBolt implements IRichBolt {
	private static final long serialVersionUID = -4881326448353584044L;
	
	private final Logger logger = LoggerFactory.getLogger(ParseBolt.class);
	
	/**
	 * The ID of the output stream containing all player sensor tuples.
	 */
	public final static String playerStreamId = "playerData";
	/**
	 * The ID of the output stream containing all ball sensor tuples.
	 */
	public final static String ballStreamId = "ballData";
	/**
	 * The ID of the output stream containing all referee sensor tuples.
	 */
	public final static String refereeStreamId = "refereeData";
	/**
	 * IDs of all player sensors (foot and hand).
	 */
	private final Set<Long> playerIds = new HashSet<Long>();
	/**
	 * IDs of all ball sensors.
	 */
	private final Set<Long> ballIds = new HashSet<Long>();
	/**
	 * IDs of all referee sensors.
	 */
	private final Set<Long> refereeIds = new HashSet<Long>();
	/**
	 * The output collector to be used.
	 */
	private OutputCollector collector;
	
	
	
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, @SuppressWarnings("hiding") OutputCollector collector) {
		this.collector = collector;
		
		HashMap<Long, String> dummy = new HashMap<Long, String>();
		Utils.fillPlayerFootSensorMap(dummy);
		Utils.fillPlayerHandSensorMap(dummy);
		this.playerIds.addAll(dummy.keySet());
		
		dummy.clear();
		Utils.fillBallSensorMap(dummy);
		this.ballIds.addAll(dummy.keySet());
		
		dummy.clear();
		Utils.fillRefereeSensorMap(dummy);
		this.refereeIds.addAll(dummy.keySet());
	}
	
	@Override
	public void execute(Tuple input) {
		try {
			String[] attributes = input.getString(1).split(",");
			
			if(attributes.length != 13) {
				this.logger.error("Invalid tuple. Should be in CSV format containing 13 entries. Dropping tuple <{}>",
					input.getString(1));
				return;
			}
			
			Long sid = new Long(Long.parseLong(attributes[0]));
			String outputStreamId;
			if(this.playerIds.contains(sid)) {
				outputStreamId = playerStreamId;
			} else if(this.ballIds.contains(sid)) {
				outputStreamId = ballStreamId;
			} else if(this.refereeIds.contains(sid)) {
				outputStreamId = refereeStreamId;
			} else {
				this.logger.warn("Unknown sensor ID; dropping tuple: <{}>", input);
				this.collector.ack(input);
				return;
			}
			
			this.collector.emit(outputStreamId, input, new Values(sid, // sid
				input.getLong(0), // ts
				new Integer(Integer.parseInt(attributes[2])), // x
				new Integer(Integer.parseInt(attributes[3])), // y
				new Integer(Integer.parseInt(attributes[4])), // z
				new Integer(Integer.parseInt(attributes[5])), // |v|
				new Integer(Integer.parseInt(attributes[6])), // |a|
				new Integer(Integer.parseInt(attributes[7])), // vx
				new Integer(Integer.parseInt(attributes[8])), // vy
				new Integer(Integer.parseInt(attributes[9])), // vz
				new Integer(Integer.parseInt(attributes[10])), // ax
				new Integer(Integer.parseInt(attributes[11])), // ay
				new Integer(Integer.parseInt(attributes[12])) // az
				));
		} catch(NumberFormatException e) {
			this.logger.error("Parsing error: Could not convert all attributes into numbers. Dropping tuple <{}>",
				input.getValue(1));
		} catch(ClassCastException e) {
			this.logger.error("Parsing error: Could not convert all attributes into numbers. Dropping tuple <{}>",
				input.getValue(1));
		}
		
		this.collector.ack(input);
	}
	
	@Override
	public void cleanup() { /* nothing to do */}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		Fields schema = new Fields("sid", "ts", "x", "y", "z", "|v|", "|a|", "vx", "vy", "vz", "ax", "ay", "az");
		declarer.declareStream(playerStreamId, schema);
		declarer.declareStream(ballStreamId, schema);
		declarer.declareStream(refereeStreamId, schema);
	}
	
	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
	
}
