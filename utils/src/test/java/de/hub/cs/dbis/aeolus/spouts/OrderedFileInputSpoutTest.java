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
package de.hub.cs.dbis.aeolus.spouts;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.stubbing.OngoingStubbing;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import de.hub.cs.dbis.aeolus.testUtils.TestSpoutOutputCollector;
import de.hub.cs.dbis.aeolus.testUtils.TimestampComperator;





/**
 * @author mjsax
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(TestOrderedFileInputSpout.class)
public class OrderedFileInputSpoutTest {
	
	private long seed;
	private Random r;
	
	
	@Before
	public void prepare() {
		this.seed = System.currentTimeMillis();
		this.r = new Random(this.seed);
		System.out.println("Test seed: " + this.seed);
	}
	
	@Test
	public void testOpenDefault() throws Exception {
		TestOrderedFileInputSpout spout = new TestOrderedFileInputSpout();
		
		Map<Object, Object> dummyConf = new HashMap<Object, Object>();
		spout.open(dummyConf, mock(TopologyContext.class), mock(SpoutOutputCollector.class));
		try {
			spout.closePartition(new Integer(0));
			Assert.fail();
		} catch(RuntimeException e) {
			// expected
		}
		
		
		
		Config conf = new Config();
		conf.put(TestOrderedFileInputSpout.NUMBER_OF_PARTITIONS, new Integer(2));
		spout.open(conf, mock(TopologyContext.class), mock(SpoutOutputCollector.class));
		try {
			spout.closePartition(new Integer(0));
			Assert.fail();
		} catch(RuntimeException e) {
			// expected
		}
		
		
		
		FileReader fileReaderMock = PowerMockito.mock(FileReader.class);
		PowerMockito.whenNew(FileReader.class).withAnyArguments().thenReturn(fileReaderMock);
		
		BufferedReader bufferedReaderMock = PowerMockito.mock(BufferedReader.class);
		PowerMockito.whenNew(BufferedReader.class).withArguments(fileReaderMock).thenReturn(bufferedReaderMock);
		
		spout.open(conf, mock(TopologyContext.class), mock(SpoutOutputCollector.class));
		Assert.assertTrue(spout.closePartition(new Integer(0)));
		try {
			spout.closePartition(new Integer(1));
			Assert.fail();
		} catch(RuntimeException e) {
			// expected
		}
	}
	
	@Test
	public void testOpenSinglePartition() throws Exception {
		TestOrderedFileInputSpout spout = new TestOrderedFileInputSpout();
		
		Config conf = new Config();
		conf.put(TestOrderedFileInputSpout.INPUT_FILE_NAME, "dummyFileName");
		
		FileReader fileReaderMock = PowerMockito.mock(FileReader.class);
		PowerMockito.whenNew(FileReader.class).withArguments("dummyFileName").thenReturn(fileReaderMock);
		
		BufferedReader bufferedReaderMock = PowerMockito.mock(BufferedReader.class);
		PowerMockito.whenNew(BufferedReader.class).withArguments(fileReaderMock).thenReturn(bufferedReaderMock);
		
		spout.open(conf, mock(TopologyContext.class), mock(SpoutOutputCollector.class));
		Assert.assertTrue(spout.closePartition(new Integer(0)));
		try {
			spout.closePartition(new Integer(1));
			Assert.fail();
		} catch(RuntimeException e) {
			// expected
		}
	}
	
	@Test
	public void testOpenMultiplePartitions() throws Exception {
		TestOrderedFileInputSpout spout = new TestOrderedFileInputSpout();
		
		Config conf = new Config();
		conf.put(TestOrderedFileInputSpout.INPUT_FILE_NAME, "dummyFileName-");
		conf.put(TestOrderedFileInputSpout.INPUT_FILE_SUFFIXES, Arrays.asList(new String[] {"1", "2", "3"}));
		
		for(int i = 1; i <= 3; ++i) {
			FileReader fileReaderMock = PowerMockito.mock(FileReader.class);
			PowerMockito.whenNew(FileReader.class).withArguments("dummyFileName-" + i).thenReturn(fileReaderMock);
			
			BufferedReader bufferedReaderMock = PowerMockito.mock(BufferedReader.class);
			PowerMockito.whenNew(BufferedReader.class).withArguments(fileReaderMock).thenReturn(bufferedReaderMock);
		}
		List<Integer> taskMock = new LinkedList<Integer>();
		taskMock.add(new Integer(0));
		TopologyContext contextMock = mock(TopologyContext.class);
		when(contextMock.getComponentTasks(anyString())).thenReturn(taskMock);
		when(new Integer(contextMock.getThisTaskIndex())).thenReturn(new Integer(0));
		
		spout.open(conf, contextMock, mock(SpoutOutputCollector.class));
		Assert.assertTrue(spout.closePartition(new Integer(0)));
		Assert.assertTrue(spout.closePartition(new Integer(1)));
		Assert.assertTrue(spout.closePartition(new Integer(2)));
		try {
			spout.closePartition(new Integer(3));
			Assert.fail();
		} catch(RuntimeException e) {
			// expected
		}
	}
	
	@Test
	public void testClosePartition() throws Exception {
		TestOrderedFileInputSpout spout = new TestOrderedFileInputSpout();
		
		Config conf = new Config();
		conf.put(TestOrderedFileInputSpout.INPUT_FILE_NAME, "dummyFileName-");
		conf.put(TestOrderedFileInputSpout.INPUT_FILE_SUFFIXES, Arrays.asList(new String[] {"1", "2", "3"}));
		
		
		BufferedReader[] readerMocks = new BufferedReader[3];
		for(int i = 1; i <= 3; ++i) {
			FileReader fileReaderMock = PowerMockito.mock(FileReader.class);
			PowerMockito.whenNew(FileReader.class).withArguments("dummyFileName-" + i).thenReturn(fileReaderMock);
			
			readerMocks[i - 1] = PowerMockito.mock(BufferedReader.class);
			PowerMockito.whenNew(BufferedReader.class).withArguments(fileReaderMock).thenReturn(readerMocks[i - 1]);
		}
		List<Integer> taskMock = new LinkedList<Integer>();
		taskMock.add(new Integer(0));
		TopologyContext contextMock = mock(TopologyContext.class);
		when(contextMock.getComponentTasks(anyString())).thenReturn(taskMock);
		when(new Integer(contextMock.getThisTaskIndex())).thenReturn(new Integer(0));
		
		spout.open(conf, contextMock, mock(SpoutOutputCollector.class));
		
		if(this.r.nextBoolean()) {
			Assert.assertTrue(spout.closePartition(new Integer(this.r.nextInt(3))));
		}
		
		spout.close();
		
		for(int i = 0; i < 3; ++i) {
			verify(readerMocks[i]).close();
		}
	}
	
	@Test
	public void testZeroPartitions() {
		TestOrderedFileInputSpout spout = new TestOrderedFileInputSpout();
		
		Config conf = new Config();
		conf.put(TestOrderedFileInputSpout.NUMBER_OF_PARTITIONS, new Integer(0));
		
		TestSpoutOutputCollector col = new TestSpoutOutputCollector();
		spout.open(conf, mock(TopologyContext.class), new SpoutOutputCollector(col));
		
		spout.nextTuple();
		spout.nextTuple();
		spout.nextTuple();
		
		Assert.assertEquals(col.output.size(), 0);
	}
	
	@Test
	public void testSingleEmptyPartition() {
		TestOrderedFileInputSpout spout = new TestOrderedFileInputSpout();
		
		Config conf = new Config();
		conf.put(TestOrderedFileInputSpout.NUMBER_OF_PARTITIONS, new Integer(1));
		
		TestSpoutOutputCollector col = new TestSpoutOutputCollector();
		spout.open(conf, mock(TopologyContext.class), new SpoutOutputCollector(col));
		
		spout.nextTuple();
		spout.nextTuple();
		spout.nextTuple();
		
		Assert.assertEquals(0, col.output.size());
	}
	
	@Test
	public void testAllPartitionsEmpty() {
		TestOrderedFileInputSpout spout = new TestOrderedFileInputSpout();
		
		Config conf = new Config();
		conf.put(TestOrderedFileInputSpout.NUMBER_OF_PARTITIONS, new Integer(3));
		
		TestSpoutOutputCollector col = new TestSpoutOutputCollector();
		spout.open(conf, mock(TopologyContext.class), new SpoutOutputCollector(col));
		
		spout.nextTuple();
		spout.nextTuple();
		spout.nextTuple();
		
		Assert.assertEquals(0, col.output.size());
	}
	
	@Test
	public void testSinglePartition() throws Exception {
		LinkedList<Values> expectedResult = new LinkedList<Values>();
		
		FileReader fileReaderMock = PowerMockito.mock(FileReader.class);
		PowerMockito.whenNew(FileReader.class).withAnyArguments().thenReturn(fileReaderMock);
		
		BufferedReader bufferedReaderMock = PowerMockito.mock(BufferedReader.class);
		PowerMockito.whenNew(BufferedReader.class).withArguments(fileReaderMock).thenReturn(bufferedReaderMock);
		
		
		final int numberOfLines = 20;
		OngoingStubbing<String> stub = when(bufferedReaderMock.readLine());
		for(int i = 0; i < numberOfLines; ++i) {
			String line = "sid" + i + "," + i + ",dummy";
			stub = stub.thenReturn(line);
			expectedResult.add(new Values(new Long(i), line));
		}
		stub = stub.thenReturn(null);
		
		Config conf = new Config();
		conf.put(TestOrderedFileInputSpout.NUMBER_OF_PARTITIONS, new Integer(1));
		
		TestOrderedFileInputSpout spout = new TestOrderedFileInputSpout();
		TestSpoutOutputCollector col = new TestSpoutOutputCollector();
		spout.open(conf, mock(TopologyContext.class), new SpoutOutputCollector(col));
		
		for(int i = 0; i < numberOfLines + 5; ++i) {
			spout.nextTuple();
		}
		
		Assert.assertEquals(expectedResult, col.output.get(Utils.DEFAULT_STREAM_ID));
	}
	
	@Test
	public void testMultiplePartitionsStrict() throws Exception {
		LinkedList<List<Object>> expectedResult = new LinkedList<List<Object>>();
		
		final int numberOfLines = 20;
		for(int i = 1; i <= 3; ++i) {
			FileReader fileReaderMock = PowerMockito.mock(FileReader.class);
			PowerMockito.whenNew(FileReader.class).withArguments("dummyFileName-" + i).thenReturn(fileReaderMock);
			
			BufferedReader bufferedReaderMock = PowerMockito.mock(BufferedReader.class);
			PowerMockito.whenNew(BufferedReader.class).withArguments(fileReaderMock).thenReturn(bufferedReaderMock);
			
			OngoingStubbing<String> stub = when(bufferedReaderMock.readLine());
			for(int j = 0; j < numberOfLines; ++j) {
				String line = "sid" + j + "," + j + ",dummy" + i;
				stub = stub.thenReturn(line);
				expectedResult.add(new Values(new Long(j), line));
			}
			stub = stub.thenReturn(null);
		}
		Collections.sort(expectedResult, new TimestampComperator());
		
		TestOrderedFileInputSpout spout = new TestOrderedFileInputSpout();
		
		Config conf = new Config();
		conf.put(TestOrderedFileInputSpout.INPUT_FILE_NAME, "dummyFileName-");
		conf.put(TestOrderedFileInputSpout.INPUT_FILE_SUFFIXES, Arrays.asList(new String[] {"1", "2", "3"}));
		
		List<Integer> taskMock = new LinkedList<Integer>();
		taskMock.add(new Integer(0));
		TopologyContext contextMock = mock(TopologyContext.class);
		when(contextMock.getComponentTasks(anyString())).thenReturn(taskMock);
		when(new Integer(contextMock.getThisTaskIndex())).thenReturn(new Integer(0));
		
		TestSpoutOutputCollector col = new TestSpoutOutputCollector();
		
		spout.open(conf, contextMock, new SpoutOutputCollector(col));
		
		for(int i = 0; i < 3 * numberOfLines + 5; ++i) {
			spout.nextTuple();
		}
		
		Assert.assertEquals(1, col.output.size());
		Assert.assertNotEquals(null, col.output.get(Utils.DEFAULT_STREAM_ID));
		Assert.assertEquals(3 * numberOfLines, col.output.get(Utils.DEFAULT_STREAM_ID).size());
		
		for(int i = 0; i < numberOfLines; ++i) {
			Set<List<Object>> expectedSubset = new HashSet<List<Object>>();
			Set<List<Object>> resultSubset = new HashSet<List<Object>>();
			for(int j = 0; j < 3; ++j) {
				expectedSubset.add(expectedResult.removeFirst());
				resultSubset.add(col.output.get(Utils.DEFAULT_STREAM_ID).removeFirst());
			}
			Assert.assertEquals(expectedSubset, resultSubset);
		}
	}
	
	@Test
	public void testMultiplePartitionsRandom() throws Exception {
		LinkedList<List<Object>> expectedResult = new LinkedList<List<Object>>();
		
		int size, number, totalInputSize = 0;
		
		final int stepSizeRange = 1 + this.r.nextInt(6);
		
		for(int i = 1; i <= 3; ++i) {
			FileReader fileReaderMock = PowerMockito.mock(FileReader.class);
			PowerMockito.whenNew(FileReader.class).withArguments("dummyFileName-" + i).thenReturn(fileReaderMock);
			
			BufferedReader bufferedReaderMock = PowerMockito.mock(BufferedReader.class);
			PowerMockito.whenNew(BufferedReader.class).withArguments(fileReaderMock).thenReturn(bufferedReaderMock);
			
			OngoingStubbing<String> stub = when(bufferedReaderMock.readLine());
			
			size = 20 + this.r.nextInt(200);
			totalInputSize += size;
			number = 0;
			for(int j = 0; j < size; ++j) {
				number += this.r.nextInt(stepSizeRange);
				String line = "sid" + j + "," + number + ",dummy" + i;
				stub = stub.thenReturn(line);
				expectedResult.add(new Values(new Long(number), line));
			}
			stub = stub.thenReturn(null);
		}
		Collections.sort(expectedResult, new TimestampComperator());
		
		TestOrderedFileInputSpout spout = new TestOrderedFileInputSpout();
		
		Config conf = new Config();
		conf.put(TestOrderedFileInputSpout.INPUT_FILE_NAME, "dummyFileName-");
		conf.put(TestOrderedFileInputSpout.INPUT_FILE_SUFFIXES, Arrays.asList(new String[] {"1", "2", "3"}));
		
		List<Integer> taskMock = new LinkedList<Integer>();
		taskMock.add(new Integer(0));
		TopologyContext contextMock = mock(TopologyContext.class);
		when(contextMock.getComponentTasks(anyString())).thenReturn(taskMock);
		when(new Integer(contextMock.getThisTaskIndex())).thenReturn(new Integer(0));
		
		TestSpoutOutputCollector col = new TestSpoutOutputCollector();
		
		spout.open(conf, contextMock, new SpoutOutputCollector(col));
		
		for(int i = 0; i < totalInputSize + 5; ++i) {
			spout.nextTuple();
		}
		
		Assert.assertEquals(1, col.output.size());
		Assert.assertNotEquals(null, col.output.get(Utils.DEFAULT_STREAM_ID));
		Assert.assertEquals(totalInputSize, col.output.get(Utils.DEFAULT_STREAM_ID).size());
		
		while(expectedResult.size() > 0) {
			Set<List<Object>> expectedSubset = new HashSet<List<Object>>();
			Set<List<Object>> resultSubset = new HashSet<List<Object>>();
			long ts;
			do {
				ts = ((Long)expectedResult.getFirst().get(0)).longValue();
				expectedSubset.add(expectedResult.removeFirst());
				resultSubset.add(col.output.get(Utils.DEFAULT_STREAM_ID).removeFirst());
			} while(expectedResult.size() > 0 && ts == ((Long)expectedResult.getFirst().get(0)).longValue());
			
			Assert.assertEquals(expectedSubset, resultSubset);
		}
	}
	
}
