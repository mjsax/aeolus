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
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import de.hub.cs.dbis.aeolus.sinks.FileSinkBolt;
import de.hub.cs.dbis.aeolus.testUtils.AbstractBoltTest;
import de.hub.cs.dbis.lrb.operators.FileReaderSpout;
import java.io.File;
import java.io.IOException;
import org.junit.Test;
import static org.junit.Assert.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.lrb.tools.StopWatch;





/**
 * 
 * @author richter
 */
public class LRBTopologyBuilderTest extends AbstractBoltTest {
	private static final Logger logger = LoggerFactory.getLogger(LRBTopologyBuilderTest.class);
	
	/**
	 * Rudimentarily tests the {@link LRBTopologyBuilder}, i.e. the result of
	 * {@link LRBTopologyBuilder#createTopology() } as this is the only the thing the class does.
	 * 
	 * @throws IOException
	 *             if creation of one of the temporary files fails
	 */
	@Test
	public void testCreateTopology() throws IOException {
		logger.debug(String.format("Test seed: %d", seed));
		int xways = 1 + r.nextInt(9); // 0 doesn't make sense
		int workers = 1 + r.nextInt(9);
		int tasks = 1 + r.nextInt(9);
		int executors = 1 + r.nextInt(9); // has to be >= 1
		int offset = 1 + r.nextInt(9);
		IRichSpout spout = new FileReaderSpout();
		StopWatch stormTimer = new StopWatch(System.currentTimeMillis());
		boolean local = r.nextBoolean();
		String topologyNamePrefix = LRBTopologyBuilderTest.class.getSimpleName();
		Config stormConfig = new Config();
		IRichBolt tollFileWriterBolt = new FileSinkBolt(File.createTempFile(
			LRBTopologyBuilderTest.class.getSimpleName(), null));
		IRichBolt accidentOutputBolt = new FileSinkBolt(File.createTempFile(
			LRBTopologyBuilderTest.class.getSimpleName(), null));
		IRichBolt accountBalanceOutputBolt = new FileSinkBolt(File.createTempFile(
			LRBTopologyBuilderTest.class.getSimpleName(), null));
		IRichBolt dailyExpenditureOutputBolt = new FileSinkBolt(File.createTempFile(
			LRBTopologyBuilderTest.class.getSimpleName(), null));
		LRBTopologyBuilder instance = new LRBTopologyBuilder(xways, workers, tasks, executors, offset, spout,
			stormConfig, tollFileWriterBolt, accidentOutputBolt, accountBalanceOutputBolt, dailyExpenditureOutputBolt);
		StormTopology result = instance.createTopology();
		assertNotNull(result);
	}
	
}
