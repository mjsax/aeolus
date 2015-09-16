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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.lrb.tools.CommandLineParser;
import backtype.storm.Config;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.IRichBolt;
import de.hub.cs.dbis.aeolus.sinks.FileSinkBolt;
import de.hub.cs.dbis.lrb.operators.FileReaderSpout;
import java.io.File;
import java.io.FileNotFoundException;

/**
 * Handles the parsing of command line arguments passed to {@link #main(java.lang.String[])
 * } and invokes a {@link LRBTopologyRunnerLocal} to run the created
 */
public class LRBTopologyMain {

	private final static Logger LOGGER = LoggerFactory.getLogger(LRBTopologyMain.class);

	public static void main(String[] args) throws Exception {

		CommandLineParser cmd = new CommandLineParser();

		LOGGER.debug("host: " + cmd.getHost() + " port: " + cmd.getPort());
		if (cmd.getOffset() != 0) {
			LOGGER.debug("using offset: " + cmd.getOffset());
		}
		int executors = cmd.getExecutors();
		if (cmd.getXways() > 1) {
			executors = (cmd.getXways() / 2) * cmd.getExecutors();
		}
		int tasks = executors * cmd.getTasks();
		int offset = cmd.getOffset();
		int xways = cmd.getXways();
		String host = cmd.getHost();
		int port = cmd.getPort();
		String histFile = cmd.getHistFile();
		String outputDir = cmd.getOutputDirectory();
		boolean local = cmd.isLocal();
		LOGGER.debug("Submit to cluster: " + local);
		boolean stormConfigDebug = cmd.isDebug();
		int workers = cmd.getWorkers();
		String topologyNamePrefix = cmd.getTopologyNamePrefix();
		int runtimeMillis = cmd.getRuntimeMillis();
		File outputDirFile = new File(outputDir);
		if (!outputDirFile.exists()) {
			LOGGER.info("creating inexisting output directory '{}'", outputDir);
			if (!outputDirFile.mkdirs()) {
				throw new RuntimeException(String.format("Failed to create output directory '{}'", outputDir));
			}
		} else if (!outputDirFile.isDirectory()) {
			throw new RuntimeException(String.format("specified output diretory '{}' is not a directory", outputDir));
		}
		main0(outputDirFile, topologyNamePrefix, xways, workers, executors, tasks, offset, runtimeMillis, local, host,
				port, histFile, stormConfigDebug);
	}

	public static void main0(File outputDirFile, String topologyNamePrefix, int xways, int workers, int executors, int tasks, int offset, int runtimeMillis, boolean local, String host, int port, String histFile, boolean stormConfigDebug)
			throws AlreadyAliveException, InvalidTopologyException, FileNotFoundException {
		String topologyName = topologyNamePrefix + "_lrbNormal_" + "_L" + xways + "_" + workers + "W_T" + tasks + "_"
				+ executors + "E_O" + offset;
		File tollOutputBoltFile = new File(outputDirFile, topologyName + "_toll");
		File accidentOutputBoltFile = new File(outputDirFile, topologyName + "_acc");
		File accountBalanceOutputBoltFile = new File(outputDirFile, topologyName + "_bal");
		File dailyExpenditureOutputBoltFile = new File(outputDirFile, topologyName + "_exp");
		IRichBolt tollOutputBolt = new FileSinkBolt(tollOutputBoltFile);
		IRichBolt accidentOutputBolt = new FileSinkBolt(accidentOutputBoltFile);
		IRichBolt accountBalanceOutputBolt = new FileSinkBolt(accountBalanceOutputBoltFile);
		IRichBolt dailyExpenditureOutputBolt = new FileSinkBolt(dailyExpenditureOutputBoltFile);
		Config conf = new Config();
		LRBTopologyBuilder lRBTopologyBuilder = new LRBTopologyBuilder(xways, workers, tasks, executors, offset,
				new FileReaderSpout(TopologyControl.SPOUT_STREAM_ID), // add AbstractOrderedFileInputSpout.INPUT_FILE_NAME
				// and
				// AbstractOrderedFileInputSpout.INPUT_FILE_SUFFIXES
				// in Config below
				conf, tollOutputBolt, accidentOutputBolt, accountBalanceOutputBolt, dailyExpenditureOutputBolt);
		StormTopology topology = lRBTopologyBuilder.createTopology();
		AbstractLRBTopologyRunner runner;
		if (local) {
			runner = new LRBTopologyRunnerLocal();
		} else {
			runner = new LRBTopologyRunnerRemote();
		}
		runner.run(topology, offset, executors, xways, host, port, histFile, outputDirFile, tasks, stormConfigDebug,
				workers, topologyName, runtimeMillis, conf);
	}

}
