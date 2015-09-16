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

import backtype.storm.Config;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import de.hub.cs.dbis.aeolus.testUtils.AbstractBoltTest;
import de.hub.cs.dbis.lrb.operators.FileReaderSpout;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.SocketException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import org.apache.commons.lang.SystemUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;





/**
 * 
 * @author richter
 */
public class LRBTopologyRunnerVagrantITCase extends AbstractBoltTest {
	private static final Logger logger = LoggerFactory.getLogger(LRBTopologyRunnerVagrantITCase.class);
	
	/**
	 * Test of submit method, of class LRBTopologyRunnerVagrant.
	 * 
	 * @throws backtype.storm.generated.AlreadyAliveException
	 * @throws backtype.storm.generated.InvalidTopologyException
	 * @throws java.io.FileNotFoundException
	 * @throws java.net.SocketException
	 */
	@Test
	public void testRun() throws AlreadyAliveException, InvalidTopologyException, FileNotFoundException,
		SocketException, IOException {
		if(!SystemUtils.IS_OS_LINUX) {
			logger.warn("skipping {} on non-Linux system", LRBTopologyRunnerVagrantITCase.class);
			return;
		}
		String histFilePath = "some-file";
		File outputDirFile = new File("some-other-file");
		int offset = r.nextInt(10);
		int executors = 1 + r.nextInt(2); // has to be >= 1; even
		// integration tests have to end at some point, so 3 VMs running in
		// parallel are really enough
		int xways = 1 + r.nextInt(2); // 0 doesn't make sense
		String host = "127.0.0.1";
		int port = 5060;
		int tasks = 1 + r.nextInt(2);
		boolean stormConfigDebug = false;
		int workers = 1 + r.nextInt(2);
		String topologyName = LRBTopologyRunnerLocalTest.class.getName();
		int runtimeMillis = 5000;
		List<String> vagrantBridgeIFacePriorities = new LinkedList<String>(Arrays.asList("wlan[0-9]+", "eth[09]+"));
		Config conf = new Config();
		IRichSpout spout = new FileReaderSpout(TopologyControl.SPOUT_STREAM_ID);
		// don't care about the bolts (Mockito mocks don't work because they're
		// not recursively serializable and implemenations require
		IRichBolt tollOutputBolt = new BoltStub();
		IRichBolt accidentOutputBolt = new BoltStub();
		IRichBolt accountBalanceOutputBolt = new BoltStub();
		IRichBolt dailyExpenditureOutputBolt = new BoltStub();
		StormTopology topology = new LRBTopologyBuilder(xways, workers, tasks, executors, offset, spout, conf,
			tollOutputBolt, accidentOutputBolt, accountBalanceOutputBolt, dailyExpenditureOutputBolt).createTopology();
		// test DHCP
		String vagrantBaseDirPath = System.getProperty("VAGRANT_BASE_DIR");
		File vagrantBaseDir;
		if(vagrantBaseDirPath != null) {
			vagrantBaseDir = new File(vagrantBaseDirPath);
		} else {
			vagrantBaseDir = File.createTempFile(LRBTopologyRunnerVagrantITCase.class.getSimpleName(), null);
			if(!vagrantBaseDir.delete()) {
				throw new IOException(String.format("deletion of temporary file '%s' failed",
					vagrantBaseDir.getAbsolutePath()));
			}
			if(!vagrantBaseDir.mkdir()) {
				throw new IOException(String.format("creation of temporary directory '%s' failed",
					vagrantBaseDir.getAbsolutePath()));
			}
		}
		LRBTopologyRunnerVagrant instance = new LRBTopologyRunnerVagrant(vagrantBaseDir, vagrantBridgeIFacePriorities,
			LRBTopologyRunnerVagrant.REMOVE_POLICY_KEEP);
		instance.run(topology, offset, executors, xways, host, port, histFilePath, outputDirFile, tasks,
			stormConfigDebug, workers, topologyName, runtimeMillis, conf);
		// @TODO
		// test static assignment
		// Set<String> vagrantBridgeIPs = null; // @TODO: retrieve based on current network configuration
		// instance = new LRBTopologyRunnerVagrant(vagrantBridgeIFacePriorities, null);
		// test ip list length check
	}
}
