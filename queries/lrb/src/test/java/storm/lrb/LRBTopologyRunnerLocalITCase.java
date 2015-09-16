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
import de.hub.cs.dbis.aeolus.sinks.FileSinkBolt;
import de.hub.cs.dbis.aeolus.testUtils.AbstractBoltTest;
import de.hub.cs.dbis.lrb.operators.FileReaderSpout;
import java.io.File;
import java.io.IOException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;





/**
 * 
 * @author richter
 */
public class LRBTopologyRunnerLocalITCase extends AbstractBoltTest {
	private static final Logger logger = LoggerFactory.getLogger(LRBTopologyRunnerLocalITCase.class);
	
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
	public void testRun() throws IOException, AlreadyAliveException, InvalidTopologyException {
		logger.debug("Test seed: {}", seed);
		
		String histFilePath = File.createTempFile("lrb-test", null).getAbsolutePath();
		// Java 6 temporary file creation workaround
		File outputDirFile = File.createTempFile("lrb-test-output-dir", null);
		if(!(outputDirFile.delete())) {
			throw new IOException(
				String.format("Could not delete temporary file '%s'", outputDirFile.getAbsolutePath()));
		}
		if(!(outputDirFile.mkdir())) {
			throw new IOException(
				String.format("Could not create temp directory '%s'", outputDirFile.getAbsolutePath()));
		}
		String outputDir = outputDirFile.getAbsolutePath();
		// @TODO: switch to java.nio.file.Files.createTempDirectory in Java 7
		int offset = r.nextInt(10);
		int executors = 1 + r.nextInt(9); // has to be >= 1
		int xways = 1 + r.nextInt(9); // 0 doesn't make sense
		String host = "127.0.0.1";
		int port = 5060;
		int tasks = 1 + r.nextInt(9);
		boolean stormConfigDebug = false;
		int workers = 1 + r.nextInt(9);
		String topologyName = LRBTopologyRunnerLocalITCase.class.getName();
		int runtimeMillis = 60000;
		Config conf = new Config();
		IRichSpout spout = new FileReaderSpout(TopologyControl.SPOUT_STREAM_ID);
		File tollOutputBoltFile = new File(outputDirFile, topologyName + "_toll");
		File accidentOutputBoltFile = new File(outputDirFile, topologyName + "_acc");
		File accountBalanceOutputBoltFile = new File(outputDir, topologyName + "_bal");
		File dailyExpenditureOutputBoltFile = new File(outputDir, topologyName + "_exp");
		IRichBolt tollOutputBolt = new FileSinkBolt(tollOutputBoltFile);
		IRichBolt accidentOutputBolt = new FileSinkBolt(accidentOutputBoltFile);
		IRichBolt accountBalanceOutputBolt = new FileSinkBolt(accountBalanceOutputBoltFile);
		IRichBolt dailyExpenditureOutputBolt = new FileSinkBolt(dailyExpenditureOutputBoltFile);
		StormTopology topology = new LRBTopologyBuilder(xways, workers, tasks, executors, offset, spout, conf,
			tollOutputBolt, accidentOutputBolt, accountBalanceOutputBolt, dailyExpenditureOutputBolt).createTopology();
		
		LRBTopologyRunnerLocal instance = new LRBTopologyRunnerLocal();
		instance.run(topology, offset, executors, xways, host, port, histFilePath, outputDirFile, tasks,
			stormConfigDebug, workers, topologyName, runtimeMillis, conf);
		
	}
	
}
