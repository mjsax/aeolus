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

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import de.hub.cs.dbis.aeolus.spouts.DataDrivenStreamRateDriverSpout;
import de.hub.cs.dbis.aeolus.spouts.DataDrivenStreamRateDriverSpout.TimeUnit;
import de.hub.cs.dbis.aeolus.utils.TimestampMerger;
import de.hub.cs.dbis.lrb.operators.DispatcherBolt;
import de.hub.cs.dbis.lrb.operators.FileReaderSpout;
import de.hub.cs.dbis.lrb.queries.utils.TopologyControl;





/**
 * {@link AbstractQuery} parsed command line parameters to correctly set up a topology.
 * 
 * @author mjsax
 */
abstract class AbstractQuery {
	
	/**
	 * Adds the actual processing bolts and sinks to the query.
	 * 
	 * @param builder
	 *            The builder that already contains a spout and a dispatcher bolt.
	 * @param outputs
	 *            The output information for sinks (eg, file paths)
	 */
	abstract void addBolts(TopologyBuilder builder, String[] outputs);
	
	/**
	 * Partial topology set up (adding spout and dispatcher bolt).
	 */
	private final StormTopology createTopology(String[] output, boolean realtime) {
		TopologyBuilder builder = new TopologyBuilder();
		
		IRichSpout spout = new FileReaderSpout();
		if(realtime) {
			spout = new DataDrivenStreamRateDriverSpout<Long>(spout, 0, TimeUnit.SECONDS);
		}
		builder.setSpout(TopologyControl.SPOUT_NAME, spout);
		
		builder.setBolt(TopologyControl.SPLIT_STREAM_BOLT_NAME, new TimestampMerger(new DispatcherBolt(), 0))
			.localOrShuffleGrouping(TopologyControl.SPOUT_NAME)
			.allGrouping(TopologyControl.SPOUT_NAME, TimestampMerger.FLUSH_STREAM_ID);
		
		this.addBolts(builder, output);
		
		return builder.createTopology();
	}
	
	/**
	 * Parsed command line arguments and executes the query.
	 * 
	 * @param args
	 *            command line arguments
	 * @param outputInfos
	 *            expected outputs
	 * 
	 * @throws InvalidTopologyException
	 * @throws AlreadyAliveException
	 */
	protected final void parseArgumentsAndRun(String[] args, String[] outputInfos) throws AlreadyAliveException,
		InvalidTopologyException {
		final Config config = new Config();
		long runtime = -1;
		boolean realtime = false;
		int index = 0;
		
		checkParameters(args, index, outputInfos);
		
		if(args[index].equals("--realtime")) {
			realtime = true;
			checkParameters(args, ++index, outputInfos);
		}
		
		if(args[index].equals("--local")) {
			checkParameters(args, ++index, outputInfos);
			
			if(args.length < index + 1 + outputInfos.length + 1) {
				showUsage(outputInfos);
			}
			runtime = 1000 * Long.parseLong(args[index + 1 + outputInfos.length]);
		}
		config.put(FileReaderSpout.INPUT_FILE_NAME, args[index]);
		
		String[] outputs = new String[outputInfos.length];
		for(int i = 0; i < outputs.length; ++i) {
			outputs[i] = args[index + 1 + i];
		}
		
		StormTopology topology = this.createTopology(outputs, realtime);
		
		if(runtime != -1) {
			LocalCluster lc = new LocalCluster();
			lc.submitTopology(TopologyControl.TOPOLOGY_NAME, config, topology);
			
			Utils.sleep(runtime);
			lc.deactivate(TopologyControl.TOPOLOGY_NAME);
			
			Utils.sleep(10000);
			lc.shutdown();
		} else {
			StormSubmitter.submitTopology(TopologyControl.TOPOLOGY_NAME, config, topology);
		}
	}
	
	private static void checkParameters(String[] args, int index, String[] outputInfos) {
		if(args.length < index + 1 + outputInfos.length) {
			showUsage(outputInfos);
		}
	}
	
	private static void showUsage(String[] outputInfos) {
		System.err.println("Missing arguments. Usage:");
		System.err.print("bin/storm jar jarfile.jar [--realtime] [--local] <input> ");
		for(String out : outputInfos) {
			System.err.print("<" + out + "> ");
		}
		System.err.println("[<runtime>]");
		System.err.println("  <runtime> is only valid AND required if '--local' is specified");
		System.exit(-1);
	}
	
}
