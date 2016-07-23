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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedList;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
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
	protected final static OptionParser parser = new OptionParser();
	
	private final static OptionSpec<Void> realtimeOption, localOption;
	private final static OptionSpec<Long> runtimeOption;
	private final static OptionSpec<String> inputOption;
	private final static OptionSpec<Integer> highwaysOption;
	
	
	
	static {
		realtimeOption = parser.accepts("realtime", "Should data be ingested accoring to event-time."
			+ " If not specified, data is ingested as fast as possible.");
		localOption = parser.accepts("local", "Local execution instead of cluster submission.");
		runtimeOption = parser.accepts("runtime", "Requires --local. Runtime until execution is stopped.")
			.withRequiredArg().describedAs("sec").ofType(Long.class);
		inputOption = parser.accepts("input", "Spout local path to input file").withRequiredArg()
			.describedAs("file/path").ofType(String.class).required();
		highwaysOption = parser
			.accepts(
				"highways",
				"Number of highways to process (L factor). "
					+ "If not specified, --input defines a single file; otherwise, --input defines file-prefix.")
			.withRequiredArg().describedAs("num").ofType(Integer.class);
	}
	
	
	
	/**
	 * Adds the actual processing bolts and sinks to the query.
	 * 
	 * @param builder
	 *            The builder that already contains a spout and a dispatcher bolt.
	 * @param outputs
	 *            The output information for sinks (ie, file paths)
	 */
	abstract void addBolts(TopologyBuilder builder, OptionSet options);
	
	/**
	 * Partial topology set up (adding spout and dispatcher bolt).
	 */
	private final StormTopology createTopology(OptionSet options, boolean realtime) {
		TopologyBuilder builder = new TopologyBuilder();
		
		IRichSpout spout = new FileReaderSpout();
		if(realtime) {
			spout = new DataDrivenStreamRateDriverSpout<Long>(spout, 0, TimeUnit.SECONDS);
		}
		final Integer dop = OperatorParallelism.get(TopologyControl.SPOUT_NAME);
		if(dop.intValue() > 1 && !options.has(highwaysOption)) {
			throw new IllegalArgumentException(
				"You configured a Spout parallelism greater than one, but provide only one input file "
					+ "(this would lead to data duplication as all Spout instances read the same file).");
		}
		builder.setSpout(TopologyControl.SPOUT_NAME, spout, dop);
		
		builder
			.setBolt(TopologyControl.SPLIT_STREAM_BOLT_NAME, new TimestampMerger(new DispatcherBolt(), 0),
				OperatorParallelism.get(TopologyControl.SPLIT_STREAM_BOLT_NAME))
			.shuffleGrouping(TopologyControl.SPOUT_NAME)
			.allGrouping(TopologyControl.SPOUT_NAME, TimestampMerger.FLUSH_STREAM_ID);
		
		this.addBolts(builder, options);
		
		return builder.createTopology();
	}
	
	/**
	 * Parsed command line arguments and executes the query.
	 * 
	 * @param args
	 *            command line arguments
	 * 
	 * @throws IOException
	 *             if the configuration file 'lrb.cfg' could not be processed
	 * @throws InvalidTopologyException
	 *             should never happen&mdash;otherwise there is a bug in the code
	 * @throws AlreadyAliveException
	 *             if the topology is already deployed
	 */
	protected final void parseArgumentsAndRun(String[] args) throws IOException, InvalidTopologyException,
		AlreadyAliveException {
		final OptionSet options;
		try {
			options = parser.parse(args);
		} catch(OptionException e) {
			parser.printHelpOn(System.err);
			throw e;
		}
		
		final Config config = new Config();
		config.put(FileReaderSpout.INPUT_FILE_NAME, options.valueOf(inputOption));
		
		
		
		if(options.has(highwaysOption)) {
			LinkedList<String> highway = new LinkedList<String>();
			final int lFactor = options.valueOf(highwaysOption).intValue();
			
			for(int i = 0; i < lFactor; ++i) {
				highway.add(new Integer(i) + ".dat");
			}
			
			config.put(FileReaderSpout.INPUT_FILE_SUFFIXES, highway);
		}
		
		
		
		if(!options.has(localOption)) {
			BufferedReader configReader = null;
			try {
				configReader = new BufferedReader(new FileReader("lrb.cfg"));
				
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
					
					if(tokens[0].equals("NIMBUS_HOST")) {
						config.put(Config.NIMBUS_HOST, tokens[1]);
					} else if(tokens[0].equals("TOPOLOGY_WORKERS")) {
						try {
							config.setNumWorkers(Integer.parseInt(tokens[1]));
						} catch(NumberFormatException e) {
							System.err.println("Invalid line: <VALUE> for key TOPOLOGY_WORKERS must be a number.");
							System.err.println("> " + line);
						}
					} else {
						try {
							OperatorParallelism.set((String)TopologyControl.class.getField(tokens[0]).get(null),
								Integer.parseInt(tokens[1]));
							
							continue; // no error -- continue to avoid printing of error message after try-catch-block
						} catch(NoSuchFieldException e) {
							// error message is printed after try-catch-block
						} catch(SecurityException e) {
							// error message is printed after try-catch-block
						} catch(NumberFormatException e) {
							System.err.println("Invalid line: <VALUE> for key <operatorName> must be a number.");
							System.err.println("> " + line);
							continue; // different error message -- continue to avoid printing of standard error message
						} catch(IllegalArgumentException e) {
							// error message is printed after try-catch-block
						} catch(IllegalAccessException e) {
							// error message is printed after try-catch-block
						}
						
						System.err
							.println("Invalid line: <KEY> (operatorName) unknown. See TopologyControl.java for valid keys.");
						System.err.println("> " + line);
					}
				}
			} finally {
				if(configReader != null) {
					configReader.close();
				}
			}
		} else if(!options.has(runtimeOption)) {
			System.err.println("Option --local required option --runtime.");
			System.exit(-1);
		}
		
		StormTopology topology = this.createTopology(options, options.has(realtimeOption));
		
		if(options.has(localOption)) {
			LocalCluster lc = new LocalCluster();
			lc.submitTopology(TopologyControl.TOPOLOGY_NAME, config, topology);
			
			Utils.sleep(1000 * options.valueOf(runtimeOption).longValue());
			lc.deactivate(TopologyControl.TOPOLOGY_NAME);
			
			Utils.sleep(10000);
			lc.shutdown();
		} else {
			StormSubmitter.submitTopology(TopologyControl.TOPOLOGY_NAME, config, topology);
		}
	}
}
