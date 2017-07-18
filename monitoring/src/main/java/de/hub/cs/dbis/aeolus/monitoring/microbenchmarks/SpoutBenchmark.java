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
package de.hub.cs.dbis.aeolus.monitoring.microbenchmarks;

import java.io.BufferedReader;
import java.io.FileReader;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.IRichSpout;
import backtype.storm.generated.Nimbus.Client;
import backtype.storm.utils.NimbusClient;
import de.hub.cs.dbis.aeolus.monitoring.MonitoringTopoloyBuilder;
import de.hub.cs.dbis.aeolus.spouts.FixedStreamRateDriverSpout;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;





public class SpoutBenchmark {
	protected final static OptionParser parser = new OptionParser();
	
	private final static OptionSpec<Integer> batchSizeOption, recordSizeOption, ingestionRateOption,
		measureThroughputOption, measureLatencyOption;
	
	static {
		batchSizeOption = parser.accepts("batchSize", "The output batch size used by the spout.").withRequiredArg()
			.describedAs("number of tuples").ofType(Integer.class).required();
		recordSizeOption = parser.accepts("recordSize", "The size of the spout output records.").withRequiredArg()
			.describedAs("bytes").ofType(Integer.class).required();
		ingestionRateOption = parser
			.accepts("ingestionRate", "The number of output record per second the spout should emit.")
			.withRequiredArg().describedAs("tps").ofType(Integer.class);
		measureThroughputOption = parser
			.accepts("measureThroughput",
				"Collect data throughput for each operator and report in specified time intervalls.").withRequiredArg()
			.describedAs("ms").ofType(Integer.class);
		measureLatencyOption = parser
			.accepts("measureLatency",
				"Collect tuples latencies and report statistics in buckets of specified number of tuples.")
			.withRequiredArg().describedAs("cnt").ofType(Integer.class);
	}
	
	public static void main(String[] args) throws Exception {
		final Config config = new Config();
		
		final OptionParser stopOptionParser = new OptionParser();
		final OptionSpec<String> stopOption = stopOptionParser
			.accepts("stop", "Deactivates and kills a running topology.").withRequiredArg().describedAs("topology-ID")
			.ofType(String.class).required();
		
		OptionSet options = null;
		try {
			options = stopOptionParser.parse(args);
		} catch(OptionException e) {
			try {
				options = parser.parse(args);
			} catch(OptionException f) {
				System.err.println(f.getMessage());
				System.err.println();
				parser.printHelpOn(System.err);
				stopOptionParser.printHelpOn(System.err);
				System.exit(-1);
			}
		}
		
		String jarFile = "target/Microbenchmarks.jar";
		BufferedReader configReader = null;
		try {
			configReader = new BufferedReader(new FileReader("micro.cfg"));
			
			String line;
			while((line = configReader.readLine()) != null) {
				line = line.trim();
				if(line.startsWith("#") || line.length() == 0) {
					continue;
				}
				String[] tokens = line.split(":");
				if(tokens.length != 2) {
					System.err.println("Invalid line: must be <KEY>:<VALUE>");
					System.err.println("> " + line);
					continue;
				}
				
				if(tokens[0].equals("JAR")) {
					jarFile = tokens[1];
				} else if(tokens[0].equals("NIMBUS_HOST")) {
					config.put(Config.NIMBUS_HOST, tokens[1]);
				} else if(tokens[0].equals("TOPOLOGY_WORKERS")) {
					try {
						config.setNumWorkers(Integer.parseInt(tokens[1]));
					} catch(NumberFormatException e) {
						System.err.println("Invalid line: <VALUE> for key TOPOLOGY_WORKERS must be a number.");
						System.err.println("> " + line);
					}
				} else {
					System.err.println("Invalid line: <KEY> unknown.");
					System.err.println("> " + line);
				}
			}
		} finally {
			if(configReader != null) {
				configReader.close();
			}
		}
		
		if(options.has(stopOption)) {
			final String topologyId = options.valueOf(stopOption);
			
			// required default configs
			config.put(Config.NIMBUS_THRIFT_PORT, 6627);
			config.put(Config.STORM_THRIFT_TRANSPORT_PLUGIN, "backtype.storm.security.auth.SimpleTransportPlugin");
			
			Client client = NimbusClient.getConfiguredClient(config).getClient();
			try {
				client.deactivate(topologyId);
				
				Thread.sleep(30 * 1000);
				
				client.killTopology(topologyId);
			} catch(Throwable e) {
				e.printStackTrace();
				System.exit(-1);
			}
		} else {
			MonitoringTopoloyBuilder builder = new MonitoringTopoloyBuilder(options.has(measureThroughputOption),
				options.has(measureThroughputOption) ? options.valueOf(measureThroughputOption) : -1,
				options.has(measureLatencyOption),
				options.has(measureLatencyOption) ? options.valueOf(measureLatencyOption) : -1);
			
			
			IRichSpout spout = new MicroSpout(options.valueOf(recordSizeOption));
			if(options.has(ingestionRateOption)) {
				spout = new FixedStreamRateDriverSpout(spout, options.valueOf(ingestionRateOption));
			}
			String spoutId = "Spout";
			builder.setBatchingSpout(spoutId, spout, options.valueOf(batchSizeOption));
			String boltId = "Bolt";
			// builder.setSink(boltId, new DummySinkBolt()).localOrShuffleGrouping(spoutId);
			builder.setSink(boltId, new DummySinkBolt()).shuffleGrouping(spoutId);
			
			StormTopology topology = builder.createTopology();
			
			if(System.getProperty("storm.jar") == null) {
				System.setProperty("storm.jar", jarFile);
			}
			
			StormSubmitter.submitTopology("SpoutMicroBenchmark", config, topology);
		}
	}
}
