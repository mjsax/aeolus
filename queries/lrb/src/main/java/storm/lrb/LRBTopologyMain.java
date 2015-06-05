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

import java.io.FileNotFoundException;
import java.util.Locale;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.lrb.bolt.TollNotificationBolt;
import storm.lrb.tools.CommandLineParser;
import storm.lrb.tools.StopWatch;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.utils.Utils;
import de.hub.cs.dbis.aeolus.queries.utils.AbstractOrderedFileInputSpout;
import de.hub.cs.dbis.lrb.operators.FileReaderSpout;





//import storm.lrb.spout.SocketClientSpout;
/**
 * This topology is equivalent to LrbXD with the difference, that "xway" and "dir" are used in fieldsgrouping (storm
 * feature) of the {@link TollNotificationBolt} as opposed to LrbXD where one field is made out of both fields two.
 */
public class LRBTopologyMain {
	
	private final static Logger LOGGER = LoggerFactory.getLogger(LRBTopologyMain.class);
	
	public static void main(String[] args) throws Exception {
		
		CommandLineParser cmd = new CommandLineParser();
		
		LOGGER.debug("host: " + cmd.getHost() + " port: " + cmd.getPort());
		LOGGER.debug("Submit to cluster: " + cmd.isSubmit());
		if(cmd.getOffset() != 0) {
			LOGGER.debug("using offset: " + cmd.getOffset());
		}
		int executors = cmd.getExecutors();
		if(cmd.getXways() > 1) {
			executors = (cmd.getXways() / 2) * cmd.getExecutors();
		}
		int tasks = executors * cmd.getTasks();
		main0(cmd.getOffset(), executors, cmd.getXways(), cmd.getHost(), cmd.getPort(), cmd.getHistFile(), tasks,
			cmd.isSubmit(), cmd.isDebug(), cmd.getWorkers(), cmd.getNameext(), cmd.getRuntimeMillis());
	}
	
	/**
	 * Encapsulation of creation of the cluster after parameter parsing.
	 * 
	 * @param offset
	 * @param executors
	 * @param xways
	 * @param host
	 * @param port
	 * @param histFile
	 * @param tasks
	 * @param submit
	 * @param stormConfigDebug
	 * @param workers
	 * @param nameext
	 * @param runtimeMillis
	 * @throws AlreadyAliveException
	 * @throws InvalidTopologyException
	 * @throws java.io.FileNotFoundException
	 */
	public static void main0(int offset, int executors, int xways, String host, int port, String histFile, int tasks, boolean submit, boolean stormConfigDebug, int workers, String nameext, int runtimeMillis)
		throws AlreadyAliveException, InvalidTopologyException, FileNotFoundException {
		StopWatch stormTimer = new StopWatch(offset);
		String topologyNamePrefix = nameext + "_lrbNormal_" + "_L" + xways + "_" + workers + "W_T" + tasks + "_"
			+ executors + "E_O" + offset;
		Config conf = new Config();
		LRBTopology lRBTopology = new LRBTopology(nameext, xways, workers, tasks, executors, offset,
			new FileReaderSpout(TopologyControl.SPOUT_STREAM_ID), // add AbstractOrderedFileInputSpout.INPUT_FILE_NAME
																	// and
																	// AbstractOrderedFileInputSpout.INPUT_FILE_SUFFIXES
																	// in Config below
			stormTimer, submit, histFile, topologyNamePrefix, conf);
		StormTopology topology = lRBTopology.getStormTopology();
		conf.setDebug(stormConfigDebug);
		conf.put(AbstractOrderedFileInputSpout.INPUT_FILE_NAME,
			LRBTopologyMain.class.getResource("/datafile20seconds.dat").getFile());
		
		Locale newLocale = new Locale("en", "US");
		LOGGER.debug(String.format("setting locale to %s", newLocale));
		Locale.setDefault(newLocale); // why??
		
		LOGGER.debug("starting cluster: " + "stormlrb" + topologyNamePrefix);
		if(submit) {
			
			conf.setNumWorkers(workers);
			conf.setNumAckers(workers);
			
			StormSubmitter.submitTopology(topologyNamePrefix, conf, topology);
			
		} else {
			
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(TopologyControl.TOPOLOGY_NAME, conf, topology);
			
			Utils.sleep(runtimeMillis);
			cluster.killTopology(TopologyControl.TOPOLOGY_NAME);
			cluster.shutdown();
		}
	}
}
