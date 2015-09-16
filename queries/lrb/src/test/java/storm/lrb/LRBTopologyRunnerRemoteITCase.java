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
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import de.hub.cs.dbis.aeolus.testUtils.AbstractBoltTest;
import de.hub.cs.dbis.lrb.operators.FileReaderSpout;
import java.io.File;
import java.io.FileNotFoundException;
import org.junit.Ignore;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;





/**
 * 
 * @author richter
 */
public class LRBTopologyRunnerRemoteITCase extends AbstractBoltTest {
	private static final Logger logger = LoggerFactory.getLogger(LRBTopologyRunnerRemoteITCase.class);
	
	/**
	 * Test of run method of class LRBTopologyRunnerRemote (implies test of submit method).
	 * 
	 * @throws backtype.storm.generated.AlreadyAliveException
	 *             if
	 *             {@link LRBTopologyRunnerRemote#run(backtype.storm.generated.StormTopology, int, int, int, java.lang.String, int, java.lang.String, java.lang.String, int, boolean, int, java.lang.String, int, backtype.storm.Config) }
	 *             throws such an exception
	 * @throws backtype.storm.generated.InvalidTopologyException
	 *             if
	 *             {@link LRBTopologyRunnerRemote#run(backtype.storm.generated.StormTopology, int, int, int, java.lang.String, int, java.lang.String, java.lang.String, int, boolean, int, java.lang.String, int, backtype.storm.Config) }
	 *             throws such an exception
	 * @throws java.io.FileNotFoundException
	 *             if
	 *             {@link LRBTopologyRunnerRemote#run(backtype.storm.generated.StormTopology, int, int, int, java.lang.String, int, java.lang.String, java.lang.String, int, boolean, int, java.lang.String, int, backtype.storm.Config) }
	 *             throws such an exception
	 */
	@Test
	@Ignore
	// same as unit test, i.e. no IT case
	public void testRun() throws AlreadyAliveException, InvalidTopologyException, FileNotFoundException {
		String histFilePath = "some-file";
		File outputDirFile = new File("some-other-file");
		int offset = r.nextInt(10);
		int executors = 1 + r.nextInt(9); // has to be >= 1
		int xways = 1 + r.nextInt(9); // 0 doesn't make sense
		String host = "127.0.0.1";
		int port = 5060;
		int tasks = r.nextInt(10);
		boolean stormConfigDebug = false;
		int workers = r.nextInt(10);
		String topologyName = LRBTopologyRunnerLocalTest.class.getName();
		int runtimeMillis = 5000;
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
		LRBTopologyRunnerRemote instance = new LRBTopologyRunnerRemote();
		PowerMockito.mockStatic(StormSubmitter.class);
		PowerMockito.doNothing().when(StormSubmitter.class);
		// `java.lang.ClassCastException:
		// class sun.security.provider.ConfigFile` seems to be caused by Java 8
		// -> added maven-enforcer-plugin restriction <= 1.7.0
		
		instance.run(topology, offset, executors, xways, host, port, histFilePath, outputDirFile, tasks,
			stormConfigDebug, workers, topologyName, runtimeMillis, conf);
		PowerMockito.verifyStatic();
	}
	
}
