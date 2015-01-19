package de.hub.cs.dbis.aeolus.monitoring;

/*
 * #%L
 * monitoring
 * $Id:$
 * $HeadURL:$
 * %%
 * Copyright (C) 2014 - 2015 Humboldt-Universität zu Berlin
 * %%
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
 * #L%
 */

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.GeneralTopologyContext;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.TupleImpl;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import de.hub.cs.dbis.aeolus.testUtils.ForwardBolt;
import de.hub.cs.dbis.aeolus.testUtils.TestOutputCollector;





/**
 * @author Matthias J. Sax
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({LoggerFactory.class})
public class TimestampOrderCheckerTest {
	private static final Map<String, Object> boltConfig = new HashMap<String, Object>();
	
	private static TimestampOrderChecker checkerFake;
	private static ForwardBolt forwarder = new ForwardBolt(new Fields("dummy"));
	private static IRichBolt boltMockStatic;
	
	private final static long seed = System.currentTimeMillis();
	private final static Random r = new Random(seed);
	private static int tsIndex;
	private static boolean duplicates;
	
	@Mock private Logger loggerMock;
	@Mock private GeneralTopologyContext contextMock;
	
	
	
	@BeforeClass
	public static void prepareStatic() {
		System.out.println("Test seed: " + seed);
		
		tsIndex = r.nextInt();
		if(tsIndex < 0) {
			tsIndex *= -1;
		}
		
		duplicates = r.nextBoolean();
		
		boltMockStatic = mock(IRichBolt.class);
		when(boltMockStatic.getComponentConfiguration()).thenReturn(boltConfig);
		
		checkerFake = new TimestampOrderChecker(boltMockStatic, tsIndex, duplicates);
	}
	
	@Before
	public void prepareTest() {
		PowerMockito.mockStatic(LoggerFactory.class);
		when(LoggerFactory.getLogger(any(Class.class))).thenReturn(this.loggerMock);
	}
	
	@Test
	public void testExecuteDetectOutOfOrderStrict() {
		boolean indexVsName = r.nextBoolean();
		
		TestOutputCollector collector = new TestOutputCollector();
		forwarder.prepare(boltConfig, null, new OutputCollector(collector));
		
		TimestampOrderChecker checker;
		if(indexVsName) {
			checker = new TimestampOrderChecker(forwarder, 0, false);
			when(this.contextMock.getComponentOutputFields(this.contextMock.getComponentId(0), null)).thenReturn(
				new Fields("ts", "dummy"));
		} else {
			checker = new TimestampOrderChecker(forwarder, "ts", false);
			when(this.contextMock.getComponentOutputFields(this.contextMock.getComponentId(0), null)).thenReturn(
				new Fields("ts", "dummy"));
		}
		
		checker.execute(new TupleImpl(this.contextMock, new Values(new Long(0), new Character(
			(char)(32 + r.nextInt(95)))), 0, null));
		checker.execute(new TupleImpl(this.contextMock, new Values(new Long(1), new Character(
			(char)(32 + r.nextInt(95)))), 0, null));
		checker.execute(new TupleImpl(this.contextMock, new Values(new Long(2), new Character(
			(char)(32 + r.nextInt(95)))), 0, null));
		verify(this.loggerMock, atMost(0)).error(anyString(), any(Class.class), any(Class.class));
		
		checker.execute(new TupleImpl(this.contextMock, new Values(new Long(2), new Character(
			(char)(32 + r.nextInt(95)))), 0, null));
		verify(this.loggerMock, atMost(1)).error(anyString(), any(Class.class), any(Class.class));
		verify(this.loggerMock).error(anyString(), eq(new Long(2)), eq(new Long(2)));
		
		checker.execute(new TupleImpl(this.contextMock, new Values(new Long(1), new Character(
			(char)(32 + r.nextInt(95)))), 0, null));
		verify(this.loggerMock, atMost(2)).error(anyString(), any(Class.class), any(Class.class));
		verify(this.loggerMock).error(anyString(), eq(new Long(2)), eq(new Long(1)));
		
		Assert.assertTrue(collector.output.get(Utils.DEFAULT_STREAM_ID).size() == 5);
		Assert.assertTrue(collector.acked.size() == 5);
		Assert.assertTrue(collector.failed.size() == 0);
	}
	
	@Test
	public void testExecuteDetectOutOfOrder() {
		boolean indexVsName = r.nextBoolean();
		
		TestOutputCollector collector = new TestOutputCollector();
		forwarder.prepare(boltConfig, null, new OutputCollector(collector));
		
		TimestampOrderChecker checker;
		if(indexVsName) {
			checker = new TimestampOrderChecker(forwarder, 0, true);
			when(this.contextMock.getComponentOutputFields(this.contextMock.getComponentId(0), null)).thenReturn(
				new Fields("ts", "dummy"));
		} else {
			checker = new TimestampOrderChecker(forwarder, "ts", true);
			when(this.contextMock.getComponentOutputFields(this.contextMock.getComponentId(0), null)).thenReturn(
				new Fields("ts", "dummy"));
		}
		
		checker.execute(new TupleImpl(this.contextMock, new Values(new Long(1), new Character(
			(char)(32 + r.nextInt(95)))), 0, null));
		checker.execute(new TupleImpl(this.contextMock, new Values(new Long(1), new Character(
			(char)(32 + r.nextInt(95)))), 0, null));
		checker.execute(new TupleImpl(this.contextMock, new Values(new Long(2), new Character(
			(char)(32 + r.nextInt(95)))), 0, null));
		verify(this.loggerMock, atMost(0)).error(anyString(), any(Class.class), any(Class.class));
		
		checker.execute(new TupleImpl(this.contextMock, new Values(new Long(1), new Character(
			(char)(32 + r.nextInt(95)))), 0, null));
		verify(this.loggerMock, atMost(1)).error(anyString(), any(Class.class), any(Class.class));
		verify(this.loggerMock).error(anyString(), eq(new Long(2)), eq(new Long(1)));
		
		Assert.assertTrue(collector.output.get(Utils.DEFAULT_STREAM_ID).size() == 4);
		Assert.assertTrue(collector.acked.size() == 4);
		Assert.assertTrue(collector.failed.size() == 0);
	}
	
	@Test
	public void testCleanup() {
		checkerFake.cleanup();
		verify(boltMockStatic).cleanup();
		verify(boltMockStatic, atMost(1)).cleanup();
	}
	
	@Test
	public void testDeclareOutputFields() {
		OutputFieldsDeclarer declarerMock = mock(OutputFieldsDeclarer.class);
		checkerFake.declareOutputFields(declarerMock);
		verify(boltMockStatic).declareOutputFields(declarerMock);
		verify(boltMockStatic, atMost(1)).declareOutputFields(any(OutputFieldsDeclarer.class));
	}
	
	@Test
	public void testGetComponentConfiguration() {
		Assert.assertSame(boltConfig, checkerFake.getComponentConfiguration());
	}
	
	@Test
	public void testPrepare() {
		Map<?, ?> config = new HashMap<Object, Object>();
		TopologyContext context = mock(TopologyContext.class);
		OutputCollector collector = mock(OutputCollector.class);
		checkerFake.prepare(config, context, collector);
		verify(boltMockStatic).prepare(config, context, collector);
		verify(boltMockStatic, atMost(1)).prepare(config, context, collector);
	}
	
}
