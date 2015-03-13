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
package de.hub.cs.dbis.aeolus.monitoring;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
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
	private static final Map<String, Object> boltConfigStatic = new HashMap<String, Object>();
	private static IRichBolt boltMockStatic;
	private static Logger loggerMockStatic;
	
	private TimestampOrderChecker checker;
	private ForwardBolt forwarder = new ForwardBolt(new Fields("dummy"));
	
	private long seed;
	private Random r;
	private int tsIndex;
	private boolean duplicates;
	
	@Mock private GeneralTopologyContext contextMock;
	
	
	
	@BeforeClass
	public static void prepareTestStatic() {
		PowerMockito.mockStatic(LoggerFactory.class);
		when(LoggerFactory.getLogger(any(Class.class))).thenReturn(loggerMockStatic);
		
		loggerMockStatic = mock(Logger.class);
		
		PowerMockito.mockStatic(LoggerFactory.class);
		when(LoggerFactory.getLogger(any(Class.class))).thenReturn(loggerMockStatic);
		
		boltMockStatic = mock(IRichBolt.class);
		when(boltMockStatic.getComponentConfiguration()).thenReturn(boltConfigStatic);
	}
	
	@Before
	public void prepareTest() {
		this.seed = System.currentTimeMillis();
		this.r = new Random(this.seed);
		System.out.println("Test seed: " + this.seed);
		
		this.tsIndex = this.r.nextInt();
		if(this.tsIndex < 0) {
			this.tsIndex *= -1;
		}
		
		this.duplicates = this.r.nextBoolean();
		
		reset(loggerMockStatic);
	}
	
	@Test
	public void testExecuteDetectOutOfOrderStrict() {
		boolean indexVsName = this.r.nextBoolean();
		
		TestOutputCollector collector = new TestOutputCollector();
		this.forwarder.prepare(boltConfigStatic, null, new OutputCollector(collector));
		
		if(indexVsName) {
			this.checker = new TimestampOrderChecker(this.forwarder, 0, false);
			when(this.contextMock.getComponentOutputFields(this.contextMock.getComponentId(0), null)).thenReturn(
				new Fields("ts", "dummy"));
		} else {
			this.checker = new TimestampOrderChecker(this.forwarder, "ts", false);
			when(this.contextMock.getComponentOutputFields(this.contextMock.getComponentId(0), null)).thenReturn(
				new Fields("ts", "dummy"));
		}
		
		this.checker.execute(new TupleImpl(this.contextMock, new Values(new Long(0), new Character((char)(32 + this.r
			.nextInt(95)))), 0, null));
		this.checker.execute(new TupleImpl(this.contextMock, new Values(new Long(1), new Character((char)(32 + this.r
			.nextInt(95)))), 0, null));
		this.checker.execute(new TupleImpl(this.contextMock, new Values(new Long(2), new Character((char)(32 + this.r
			.nextInt(95)))), 0, null));
		verify(loggerMockStatic, atMost(0)).error(anyString(), any(Class.class), any(Class.class));
		
		this.checker.execute(new TupleImpl(this.contextMock, new Values(new Long(2), new Character((char)(32 + this.r
			.nextInt(95)))), 0, null));
		verify(loggerMockStatic, atMost(1)).error(anyString(), any(Class.class), any(Class.class));
		verify(loggerMockStatic).error(anyString(), eq(new Long(2)), eq(new Long(2)));
		
		this.checker.execute(new TupleImpl(this.contextMock, new Values(new Long(1), new Character((char)(32 + this.r
			.nextInt(95)))), 0, null));
		verify(loggerMockStatic, atMost(2)).error(anyString(), any(Class.class), any(Class.class));
		verify(loggerMockStatic).error(anyString(), eq(new Long(2)), eq(new Long(1)));
		
		Assert.assertEquals(5, collector.output.get(Utils.DEFAULT_STREAM_ID).size());
		Assert.assertEquals(5, collector.acked.size());
		Assert.assertEquals(0, collector.failed.size());
	}
	
	@Test
	public void testExecuteDetectOutOfOrder() {
		boolean indexVsName = this.r.nextBoolean();
		
		TestOutputCollector collector = new TestOutputCollector();
		this.forwarder.prepare(boltConfigStatic, null, new OutputCollector(collector));
		
		if(indexVsName) {
			this.checker = new TimestampOrderChecker(this.forwarder, 0, true);
			when(this.contextMock.getComponentOutputFields(this.contextMock.getComponentId(0), null)).thenReturn(
				new Fields("ts", "dummy"));
		} else {
			this.checker = new TimestampOrderChecker(this.forwarder, "ts", true);
			when(this.contextMock.getComponentOutputFields(this.contextMock.getComponentId(0), null)).thenReturn(
				new Fields("ts", "dummy"));
		}
		
		this.checker.execute(new TupleImpl(this.contextMock, new Values(new Long(1), new Character((char)(32 + this.r
			.nextInt(95)))), 0, null));
		this.checker.execute(new TupleImpl(this.contextMock, new Values(new Long(1), new Character((char)(32 + this.r
			.nextInt(95)))), 0, null));
		this.checker.execute(new TupleImpl(this.contextMock, new Values(new Long(2), new Character((char)(32 + this.r
			.nextInt(95)))), 0, null));
		verify(loggerMockStatic, atMost(0)).error(anyString(), any(Class.class), any(Class.class));
		
		this.checker.execute(new TupleImpl(this.contextMock, new Values(new Long(1), new Character((char)(32 + this.r
			.nextInt(95)))), 0, null));
		verify(loggerMockStatic, atMost(1)).error(anyString(), any(Class.class), any(Class.class));
		verify(loggerMockStatic).error(anyString(), eq(new Long(2)), eq(new Long(1)));
		
		Assert.assertEquals(4, collector.output.get(Utils.DEFAULT_STREAM_ID).size());
		Assert.assertEquals(4, collector.acked.size());
		Assert.assertEquals(0, collector.failed.size());
	}
	
	@Test
	public void testCleanup() {
		this.checker = new TimestampOrderChecker(boltMockStatic, this.tsIndex, this.duplicates);
		
		this.checker.cleanup();
		verify(boltMockStatic).cleanup();
		verify(boltMockStatic, atMost(1)).cleanup();
	}
	
	@Test
	public void testDeclareOutputFields() {
		this.checker = new TimestampOrderChecker(boltMockStatic, this.tsIndex, this.duplicates);
		
		OutputFieldsDeclarer declarerMock = mock(OutputFieldsDeclarer.class);
		this.checker.declareOutputFields(declarerMock);
		verify(boltMockStatic).declareOutputFields(declarerMock);
		verify(boltMockStatic, atMost(1)).declareOutputFields(any(OutputFieldsDeclarer.class));
	}
	
	@Test
	public void testGetComponentConfiguration() {
		this.checker = new TimestampOrderChecker(boltMockStatic, this.tsIndex, this.duplicates);
		
		Assert.assertSame(boltConfigStatic, this.checker.getComponentConfiguration());
	}
	
	@Test
	public void testPrepare() {
		this.checker = new TimestampOrderChecker(boltMockStatic, this.tsIndex, this.duplicates);
		
		Map<?, ?> config = new HashMap<Object, Object>();
		TopologyContext context = mock(TopologyContext.class);
		OutputCollector collector = mock(OutputCollector.class);
		this.checker.prepare(config, context, collector);
		verify(boltMockStatic).prepare(config, context, collector);
		verify(boltMockStatic, atMost(1)).prepare(config, context, collector);
	}
	
}
