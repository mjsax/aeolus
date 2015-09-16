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
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import de.hub.cs.dbis.aeolus.testUtils.AbstractBoltTest;
import de.hub.cs.dbis.lrb.operators.FileReaderSpout;
import java.io.File;
import java.io.IOException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;





/**
 * 
 * @author richter
 */
@PrepareForTest({StormSubmitter.class, LRBTopologyRunnerLocal.class, LRBTopologyRunnerLocalTest.class,
	LocalCluster.class})
@RunWith(PowerMockRunner.class)
public class LRBTopologyRunnerLocalTest extends AbstractBoltTest {
	private static final Logger logger = LoggerFactory.getLogger(LRBTopologyRunnerLocalTest.class);
	
	/**
	 * Test of run method, of class LRBTopologyRunnerLocal.
	 * 
	 * @throws java.io.IOException
	 *             if the creation of one of the temporary files used in this test fails
	 * @throws backtype.storm.generated.AlreadyAliveException
	 *             if
	 *             {@link LRBTopologyRunner#run(backtype.storm.generated.StormTopology, int, int, int, java.lang.String, int, java.lang.String, java.lang.String, int, boolean, boolean, int, java.lang.String, int, backtype.storm.Config) }
	 *             thrown an unexpected {@code AlreadyAliveException}
	 * @throws backtype.storm.generated.InvalidTopologyException
	 *             if
	 *             {@link LRBTopologyRunner#run(backtype.storm.generated.StormTopology, int, int, int, java.lang.String, int, java.lang.String, java.lang.String, int, boolean, boolean, int, java.lang.String, int, backtype.storm.Config) }
	 *             thrown an unexpected {@code InvalidTopologyException}
	 */
	@Test
	public void testRun() throws IOException, AlreadyAliveException, InvalidTopologyException, Exception {
		String histFilePath = "some-file";
		File outputDirFile = new File("some-other-file");
		int offset = r.nextInt(10);
		int executors = 1 + r.nextInt(9); // has to be >= 1
		int xways = 1 + r.nextInt(9); // 0 doesn't make sense
		String host = "127.0.0.1";
		int port = 5060;
		int tasks = 1 + r.nextInt(9);
		boolean stormConfigDebug = false;
		int workers = 1 + r.nextInt(9);
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
		
		LRBTopologyRunnerLocal instance = new LRBTopologyRunnerLocal();
		PowerMockito.whenNew(LocalCluster.class).withAnyArguments().thenReturn(PowerMockito.mock(LocalCluster.class));
		instance.run(topology, offset, executors, xways, host, port, histFilePath, outputDirFile, tasks,
			stormConfigDebug, workers, topologyName, runtimeMillis, conf);
		PowerMockito.verifyNew(LocalCluster.class).withNoArguments();
		// `java.lang.ClassCastException:
		// class sun.security.provider.ConfigFile` seems to be caused by Java 8
		// -> added maven-enforcer-plugin restriction <= 1.7.0
		
		
		// @TODO: validate executors <= 0 + constraints of other args
	}
	
}
