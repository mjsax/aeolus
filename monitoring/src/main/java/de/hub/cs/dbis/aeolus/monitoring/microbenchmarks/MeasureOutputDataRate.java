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
package de.hub.cs.dbis.aeolus.monitoring.microbenchmarks;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashMap;

import org.apache.thrift7.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.ExecutorSummary;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.KillOptions;
import backtype.storm.generated.Nimbus.Client;
import backtype.storm.generated.NotAliveException;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;
import de.hub.cs.dbis.aeolus.batching.InputDebatcher;
import de.hub.cs.dbis.aeolus.batching.SpoutOutputBatcher;
import de.hub.cs.dbis.aeolus.monitoring.throughput.AbstractThroughputCounter;
import de.hub.cs.dbis.aeolus.monitoring.throughput.ThroughputCounterBolt;
import de.hub.cs.dbis.aeolus.monitoring.throughput.ThroughputCounterSpout;
import de.hub.cs.dbis.aeolus.monitoring.utils.AeolusConfig;
import de.hub.cs.dbis.aeolus.monitoring.utils.ConfigReader;
import de.hub.cs.dbis.aeolus.queries.utils.FileFlushSinkBolt;
import de.hub.cs.dbis.aeolus.queries.utils.FixedStreamRateDriverSpout;
import de.hub.cs.dbis.aeolus.testUtils.ForwardBolt;





/**
 * TODO
 * 
 * @author Matthias J. Sax
 */
public class MeasureOutputDataRate {
	private final static Logger logger = LoggerFactory.getLogger(MeasureOutputDataRate.class);
	
	/**
	 * TODO
	 * 
	 * @throws IOException
	 * @throws InvalidTopologyException
	 * @throws AlreadyAliveException
	 * @throws TException
	 * @throws NotAliveException
	 * 
	 */
	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws IOException, AlreadyAliveException, InvalidTopologyException,
		NotAliveException, TException {
		final String spoutId = "Spout";
		final String sinkId = "Sink";
		final String spoutStatisticsId = "SpoutStats";
		final String sinkStatisticsId = "BoltStats";
		final String topologyId = "microbenchmark-MeasureOutputDataRate";
		final String spoutStatsFile = "/tmp/aeolus-spout.stats";
		final String sinkStatsFile = "/tmp/aeolus-sink.stats";
		
		String aeolusConfigFile = null;
		AeolusConfig aeolusConfig = null;
		
		boolean submitOrTerminate = true; // true => submit; false => terminate
		
		int i = -1;
		while(++i < args.length) {
			if(args[i].equals("-h") || args[i].equals("--help")) {
				printHelp();
				return;
			} else if(args[i].equals("-c")) {
				++i;
				if(i == args.length) {
					System.err.println("flag -c found but no <filename> was specified");
					return;
				}
				aeolusConfigFile = args[i];
			} else if(args[i].equals("--kill")) {
				submitOrTerminate = false;
			} else {
				System.err.println("unknown flag " + args[i]);
				return;
			}
		}
		
		// add $HOME/.storm as system property, such that StormSubmitter can look for storm.yaml there
		String userHome = System.getProperties().getProperty("user.home");
		if(userHome != null) {
			try {
				Method method = URLClassLoader.class.getDeclaredMethod("addURL", new Class[] {URL.class});
				method.setAccessible(true);
				method.invoke(ClassLoader.getSystemClassLoader(), new Object[] {new File(userHome + File.separator
					+ ".storm").toURI().toURL()});
			} catch(NoSuchMethodException e) {
				logger.debug("Could not add $HOME/.storm as system resource.", e);
			} catch(SecurityException e) {
				logger.debug("Could not add $HOME/.storm as system resource.", e);
			} catch(IllegalArgumentException e) {
				logger.debug("Could not add $HOME/.storm as system resource.", e);
			} catch(MalformedURLException e) {
				logger.debug("Could not add $HOME/.storm as system resource.", e);
			} catch(IllegalAccessException e) {
				logger.debug("Could not add $HOME/.storm as system resource.", e);
			} catch(InvocationTargetException e) {
				logger.debug("Could not add $HOME/.storm as system resource.", e);
			}
		}
		
		if(System.getProperty("storm.jar") == null) {
			System.setProperty("storm.jar", "target/monitoring-1.0-SNAPSHOT-microbenchmarks.jar");
		}
		
		if(aeolusConfigFile != null) {
			aeolusConfig = ConfigReader.readConfig(aeolusConfigFile);
		} else {
			try {
				// default configuration directory within maven project
				aeolusConfig = ConfigReader.readConfig("src/main/resources/");
				logger.trace("using {} from src/main/recources", ConfigReader.defaultConfigFile);
			} catch(FileNotFoundException e) {
				logger.debug("{} not found in src/main/recources/", ConfigReader.defaultConfigFile);
				try {
					// if started outside maven project, look at current working directory
					aeolusConfig = ConfigReader.readConfig();
					logger.trace("using local {}", ConfigReader.defaultConfigFile);
				} catch(FileNotFoundException f) {
					logger.debug("{} not found in local working directory (.)", ConfigReader.defaultConfigFile);
				}
			}
		}
		
		Config stormConfig = new Config();
		stormConfig.putAll(Utils.readStormConfig());
		
		// Aeolus configuration overwrites Storm configuration
		if(aeolusConfig != null) {
			// add only if not null, otherwise StormSubmitter cannot add values from storm.yaml
			String nimbusHost = aeolusConfig.getNimbusHost();
			if(nimbusHost != null) {
				logger.trace("using nimbus.host from {}", ConfigReader.defaultConfigFile);
				stormConfig.put(Config.NIMBUS_HOST, nimbusHost);
			}
			Integer nimbusPort = aeolusConfig.getNimbusPort();
			if(nimbusPort != null) {
				logger.trace("using nimbus.port from {}", ConfigReader.defaultConfigFile);
				stormConfig.put(Config.NIMBUS_THRIFT_PORT, nimbusPort);
			}
		}
		// command line arguments overwrite everything
		stormConfig.putAll(Utils.readCommandLineOpts());
		
		Client client = NimbusClient.getConfiguredClient(stormConfig).getClient();
		if(submitOrTerminate) {
			final double dataRate = Double.parseDouble(System.getProperty("aeolus.microbenchmarks.dataRate"));
			final int batchSize = Integer.parseInt(System.getProperty("aeolus.microbenchmarks.batchSize"));
			final int interval = Integer.parseInt(System.getProperty("aeolus.microbenchmarks.reportingInterval"));
			
			TopologyBuilder builder = new TopologyBuilder();
			
			// spout
			IRichSpout spout = new ThroughputCounterSpout(new FixedStreamRateDriverSpout(new SchemaSpout(), dataRate),
				interval);
			if(batchSize > 0) {
				HashMap<String, Integer> batchSizes = new HashMap<String, Integer>();
				batchSizes.put(Utils.DEFAULT_STREAM_ID, new Integer(batchSize));
				spout = new SpoutOutputBatcher(spout, batchSizes);
			}
			builder.setSpout(spoutId, spout);
			
			// sink
			IRichBolt sink = new ThroughputCounterBolt(new ForwardBolt(), interval, true);
			if(batchSize > 0) {
				sink = new InputDebatcher(sink);
			}
			builder.setBolt(sinkId, sink).shuffleGrouping(spoutId);
			
			// statistics
			builder.setBolt(spoutStatisticsId, new FileFlushSinkBolt(spoutStatsFile)).shuffleGrouping(spoutId,
				AbstractThroughputCounter.DEFAULT_STATS_STREAM);
			builder.setBolt(sinkStatisticsId, new FileFlushSinkBolt(sinkStatsFile)).shuffleGrouping(sinkId,
				AbstractThroughputCounter.DEFAULT_STATS_STREAM);
			
			stormConfig.setNumWorkers(4);
			
			// stormConfig.setFallBackOnJavaSerialization(false);
			// stormConfig.setSkipMissingKryoRegistrations(false);
			// stormConfig.put(Config.STORM_THRIFT_TRANSPORT_PLUGIN,
			// "backtype.storm.security.auth.SimpleTransportPlugin");
			// stormConfig.put("storm.thrift.transport", "backtype.storm.security.auth.SimpleTransportPlugin");
			SpoutOutputBatcher.registerKryoClasses(stormConfig);
			StormSubmitter.submitTopology(topologyId, stormConfig, builder.createTopology());
			// LocalCluster c = new LocalCluster();
			// c.submitTopology(topologyId, stormConfig, builder.createTopology());
			
			String id = client.getClusterInfo().get_topologies().get(0).get_id();
			TopologyInfo info = client.getTopologyInfo(id);
			
			String spoutHost = null, sinkHost = null, spoutStatsHost = null, sinkStatsHost = null;
			for(ExecutorSummary executor : info.get_executors()) {
				String operatorId = executor.get_component_id();
				if(operatorId.equals(spoutId)) {
					spoutHost = executor.get_host();
					if(spoutHost.equals(sinkHost)) {
						throw new RuntimeException("spout and sink deployed at same host");
					}
				} else if(operatorId.equals(sinkId)) {
					sinkHost = executor.get_host();
					if(sinkHost.equals(spoutHost)) {
						throw new RuntimeException("spout and sink deployed at same host");
					}
				} else if(operatorId.equals(spoutStatisticsId)) {
					spoutStatsHost = executor.get_host();
				} else if(operatorId.equals(sinkStatisticsId)) {
					sinkStatsHost = executor.get_host();
				}
			}
			System.out.println("Aeolus.MeasureOutputDataRate.spoutHost=" + spoutHost);
			System.out.println("Aeolus.MeasureOutputDataRate.spoutStatsHost=" + spoutStatsHost);
			System.out.println("Aeolus.MeasureOutputDataRate.spoutStatsFile=" + spoutStatsFile);
			System.out.println("Aeolus.MeasureOutputDataRate.sinkStatsHost=" + sinkStatsHost);
			System.out.println("Aeolus.MeasureOutputDataRate.sinkStatsFile=" + sinkStatsFile);
			// c.killTopology(topologyId);
			// Utils.sleep(1000);
			// c.shutdown();
		} else {
			KillOptions killOptions = new KillOptions();
			killOptions.set_wait_secs(0);
			
			client.killTopologyWithOpts(topologyId, killOptions);
		}
	}
	
	/**
	 * Prints a help dialog to stdout.
	 */
	private static void printHelp() {
		System.out.println("MeassureOutputDateRate support the following flag:");
		System.out.println(" -h | --help\tshows this help");
		System.out.println(" -c <filename>\tsets an alternative Aeolus configuration file");
		System.out
			.println(" --kill\tTerminates the benchmark topology. If not present, the benchmark will be started.");
	}
	
}
