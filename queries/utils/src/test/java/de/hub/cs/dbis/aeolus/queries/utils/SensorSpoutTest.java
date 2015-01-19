/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package debs2013;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.stubbing.OngoingStubbing;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import de.hub.cs.dbis.aeolus.testUtils.TestDeclarer;
import de.hub.cs.dbis.aeolus.testUtils.TestSpoutOutputCollector;





/**
 * @author Matthias J. Sax
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(SensorSpout.class)
public class SensorSpoutTest {
	private final static List<List<Object>> result = new LinkedList<List<Object>>();
	
	private final static long seed = System.currentTimeMillis();
	private final static Random r = new Random(seed);
	
	
	
	@BeforeClass
	public static void prepareStatic() {
		System.out.println("Test seed: " + seed);
	}
	
	
	
	@Test
	public void testNextTupleDefaultInputFile() throws Exception {
		FileReader fileReaderMock = PowerMockito.mock(FileReader.class);
		PowerMockito.whenNew(FileReader.class).withAnyArguments().thenReturn(fileReaderMock);
		
		BufferedReader bufferedReaderMock = PowerMockito.mock(BufferedReader.class);
		PowerMockito.whenNew(BufferedReader.class).withArguments(fileReaderMock).thenReturn(bufferedReaderMock);
		
		SensorSpout spout = new SensorSpout();
		TestSpoutOutputCollector collector = new TestSpoutOutputCollector();
		
		final int numberOfLines = 20;
		result.clear();
		OngoingStubbing<String> stub = when(bufferedReaderMock.readLine());
		for(int i = 0; i < numberOfLines; ++i) {
			String line = "sid" + i + "," + i + ",dummy";
			stub = stub.thenReturn(line);
			result.add(new Values(new Long(i), line));
		}
		
		spout.open(mock(Config.class), null, new SpoutOutputCollector(collector));
		for(int i = 0; i < numberOfLines; ++i) {
			spout.nextTuple();
		}
		
		Assert.assertEquals(result.subList(0, result.size()), collector.output.get(Utils.DEFAULT_STREAM_ID));
	}
	
	@Test
	public void testNextTupleUserInputFile() throws Exception {
		final String fileName = "testDummyFileName";
		
		FileReader fileReaderMock = PowerMockito.mock(FileReader.class);
		PowerMockito.whenNew(FileReader.class).withArguments(fileName).thenReturn(fileReaderMock);
		
		BufferedReader bufferedReaderMock = PowerMockito.mock(BufferedReader.class);
		PowerMockito.whenNew(BufferedReader.class).withArguments(fileReaderMock).thenReturn(bufferedReaderMock);
		
		SensorSpout spout = new SensorSpout();
		Config conf = new Config();
		conf.put(SensorSpout.INPUT_FILE_NAME, fileName);
		spout.open(conf, null, null);
		TestSpoutOutputCollector collector = new TestSpoutOutputCollector();
		
		final int numberOfLines = 20;
		result.clear();
		OngoingStubbing<String> stub = when(bufferedReaderMock.readLine());
		for(int i = 0; i < numberOfLines; ++i) {
			String line = "sid" + i + "," + i + ",dummy";
			stub = stub.thenReturn(line);
			result.add(new Values(new Long(i), line));
		}
		
		spout.open(mock(Config.class), null, new SpoutOutputCollector(collector));
		for(int i = 0; i < numberOfLines; ++i) {
			spout.nextTuple();
		}
		
		Assert.assertEquals(result.subList(0, result.size()), collector.output.get(Utils.DEFAULT_STREAM_ID));
	}
	
	@SuppressWarnings("boxing")
	@Test
	public void testNextTupleSuffixInputFile() throws Exception {
		final String suffix = "Suffix";
		
		LinkedList<String> suffixes = new LinkedList<String>();
		suffixes.add(suffix);
		
		FileReader fileReaderMock = PowerMockito.mock(FileReader.class);
		PowerMockito.whenNew(FileReader.class).withArguments("sensorSuffix").thenReturn(fileReaderMock);
		
		BufferedReader bufferedReaderMock = PowerMockito.mock(BufferedReader.class);
		PowerMockito.whenNew(BufferedReader.class).withArguments(fileReaderMock).thenReturn(bufferedReaderMock);
		
		List<Integer> taskMock = new LinkedList<Integer>();
		taskMock.add(new Integer(0));
		TopologyContext contextMock = mock(TopologyContext.class);
		when(contextMock.getComponentTasks(anyString())).thenReturn(taskMock);
		when(contextMock.getThisTaskIndex()).thenReturn(0);
		
		final int numberOfLines = 20;
		result.clear();
		OngoingStubbing<String> stub = when(bufferedReaderMock.readLine());
		for(int i = 0; i < numberOfLines; ++i) {
			String line = "sid" + i + "," + i + ",dummy";
			stub = stub.thenReturn(line);
			result.add(new Values(new Long(i), line));
		}
		
		SensorSpout spout = new SensorSpout();
		Config conf = new Config();
		conf.put(SensorSpout.INPUT_FILE_SUFFIXES, suffixes);
		TestSpoutOutputCollector collector = new TestSpoutOutputCollector();
		spout.open(conf, contextMock, new SpoutOutputCollector(collector));
		for(int i = 0; i < numberOfLines; ++i) {
			spout.nextTuple();
		}
		
		Assert.assertEquals(result.subList(0, result.size()), collector.output.get(Utils.DEFAULT_STREAM_ID));
	}
	
	@Test
	public void testNextTupleFormatError() throws Exception {
		FileReader fileReaderMock = PowerMockito.mock(FileReader.class);
		PowerMockito.whenNew(FileReader.class).withAnyArguments().thenReturn(fileReaderMock);
		
		BufferedReader bufferedReaderMock = PowerMockito.mock(BufferedReader.class);
		PowerMockito.whenNew(BufferedReader.class).withArguments(fileReaderMock).thenReturn(bufferedReaderMock);
		
		SensorSpout spout = new SensorSpout();
		TestSpoutOutputCollector collector = new TestSpoutOutputCollector();
		
		when(bufferedReaderMock.readLine()).thenReturn("noCommas").thenReturn(",noNumber,").thenReturn(null);
		
		spout.open(mock(Config.class), null, new SpoutOutputCollector(collector));
		spout.nextTuple();
		spout.nextTuple();
		
		Assert.assertNull(collector.output.get(Utils.DEFAULT_STREAM_ID));
	}
	
	@SuppressWarnings({"unchecked", "boxing"})
	@Test
	public void testNextTupleMultipleInputFilesStrict() throws Exception {
		final int numberOfInputFiles = 2 + r.nextInt(4);
		final int numberOfTasks = 1 + r.nextInt(5);
		final int numberOfLines = numberOfInputFiles * (5 + r.nextInt(16));
		
		final String filePrefix = "dummyName";
		BufferedReader[] bufferedReaderMock = new BufferedReader[numberOfInputFiles];
		FileReader[] fileReaderMock = new FileReader[numberOfInputFiles];
		TopologyContext[] contextMocks = new TopologyContext[numberOfTasks];
		SensorSpout[] spoutTasks = new SensorSpout[numberOfTasks];
		TestSpoutOutputCollector[] collectors = new TestSpoutOutputCollector[numberOfTasks];
		
		
		Config conf = new Config();
		conf.put(SensorSpout.INPUT_FILE_NAME, filePrefix);
		
		LinkedList<String> suffixes = new LinkedList<String>();
		for(int i = 0; i < numberOfInputFiles; ++i) {
			suffixes.add("-" + i);
			
			fileReaderMock[i] = PowerMockito.mock(FileReader.class);
			PowerMockito.whenNew(FileReader.class).withArguments(filePrefix + suffixes.getLast())
				.thenReturn(fileReaderMock[i]);
			
			bufferedReaderMock[i] = PowerMockito.mock(BufferedReader.class);
			PowerMockito.whenNew(BufferedReader.class).withArguments(fileReaderMock[i])
				.thenReturn(bufferedReaderMock[i]);
		}
		conf.put(SensorSpout.INPUT_FILE_SUFFIXES, suffixes);
		
		List<Integer> taskMock = new LinkedList<Integer>();
		for(int i = 0; i < numberOfTasks; ++i) {
			taskMock.add(new Integer(i));
			
			contextMocks[i] = mock(TopologyContext.class);
			when(contextMocks[i].getComponentTasks(anyString())).thenReturn(taskMock);
			when(contextMocks[i].getThisTaskIndex()).thenReturn(i);
			
			spoutTasks[i] = new SensorSpout();
			collectors[i] = new TestSpoutOutputCollector();
		}
		
		
		
		result.clear();
		OngoingStubbing<?>[] stub = new OngoingStubbing[numberOfInputFiles];
		
		for(int i = 0; i < numberOfLines; ++i) {
			String line = "sid" + i + "," + i + ",dummy";
			
			int inputIndex = r.nextInt(numberOfInputFiles);
			if(stub[inputIndex] == null) {
				stub[inputIndex] = when(bufferedReaderMock[inputIndex].readLine()).thenReturn(line);
			} else {
				stub[inputIndex] = ((OngoingStubbing<String>)stub[inputIndex]).thenReturn(line);
			}
			result.add(new Values(new Long(i), line));
		}
		for(int i = 0; i < numberOfInputFiles; ++i) {
			((OngoingStubbing<String>)stub[i]).thenReturn(null);
		}
		
		LinkedList<Integer> validTaskIndex = new LinkedList<Integer>();
		for(int i = 0; i < numberOfTasks; ++i) {
			spoutTasks[i].open(conf, contextMocks[i], new SpoutOutputCollector(collectors[i]));
			validTaskIndex.add(new Integer(i));
		}
		
		int[] invocationCount = new int[numberOfTasks];
		while(validTaskIndex.size() > 0) {
			int validIndex = r.nextInt(validTaskIndex.size());
			int index = validTaskIndex.get(validIndex);
			spoutTasks[index].nextTuple();
			if(++invocationCount[index] == numberOfLines) {
				validTaskIndex.remove(validIndex);
			}
		}
		
		for(List<Object> tuple : result) {
			boolean found = false;
			
			for(int i = 0; i < numberOfTasks; ++i) {
				if(collectors[i].output.get(Utils.DEFAULT_STREAM_ID).size() > 0
					&& collectors[i].output.get(Utils.DEFAULT_STREAM_ID).getFirst().equals(tuple)) {
					collectors[i].output.get(Utils.DEFAULT_STREAM_ID).removeFirst();
					found = true;
					break;
				}
			}
			
			Assert.assertTrue(found);
		}
		for(int i = 0; i < numberOfTasks; ++i) {
			Assert.assertTrue(collectors[i].output.size() == 1);
			Assert.assertTrue(collectors[i].output.get(Utils.DEFAULT_STREAM_ID).size() == 0);
		}
	}
	
	@SuppressWarnings({"unchecked", "boxing"})
	@Test
	public void testNextTupleMultipleInputFiles() throws Exception {
		final int numberOfInputFiles = 2 + r.nextInt(4);
		final int numberOfTasks = 1 + r.nextInt(5);
		final int numberOfLines = numberOfInputFiles * (5 + r.nextInt(16));
		
		final String filePrefix = "dummyName";
		BufferedReader[] bufferedReaderMock = new BufferedReader[numberOfInputFiles];
		FileReader[] fileReaderMock = new FileReader[numberOfInputFiles];
		TopologyContext[] contextMocks = new TopologyContext[numberOfTasks];
		SensorSpout[] spoutTasks = new SensorSpout[numberOfTasks];
		TestSpoutOutputCollector[] collectors = new TestSpoutOutputCollector[numberOfTasks];
		
		
		Config conf = new Config();
		conf.put(SensorSpout.INPUT_FILE_NAME, filePrefix);
		
		LinkedList<String> suffixes = new LinkedList<String>();
		for(int i = 0; i < numberOfInputFiles; ++i) {
			suffixes.add("-" + i);
			
			fileReaderMock[i] = PowerMockito.mock(FileReader.class);
			PowerMockito.whenNew(FileReader.class).withArguments(filePrefix + suffixes.getLast())
				.thenReturn(fileReaderMock[i]);
			
			bufferedReaderMock[i] = PowerMockito.mock(BufferedReader.class);
			PowerMockito.whenNew(BufferedReader.class).withArguments(fileReaderMock[i])
				.thenReturn(bufferedReaderMock[i]);
		}
		conf.put(SensorSpout.INPUT_FILE_SUFFIXES, suffixes);
		
		List<Integer> taskMock = new LinkedList<Integer>();
		for(int i = 0; i < numberOfTasks; ++i) {
			taskMock.add(new Integer(i));
			
			contextMocks[i] = mock(TopologyContext.class);
			when(contextMocks[i].getComponentTasks(anyString())).thenReturn(taskMock);
			when(contextMocks[i].getThisTaskIndex()).thenReturn(i);
			
			spoutTasks[i] = new SensorSpout();
			collectors[i] = new TestSpoutOutputCollector();
		}
		
		
		
		result.clear();
		OngoingStubbing<?>[] stub = new OngoingStubbing[numberOfInputFiles];
		
		int ts = 0;
		for(int i = 0; i < numberOfLines; ++i) {
			String line = "sid" + i + "," + ts + ",dummy";
			
			int inputIndex = r.nextInt(numberOfInputFiles);
			if(stub[inputIndex] == null) {
				stub[inputIndex] = when(bufferedReaderMock[inputIndex].readLine()).thenReturn(line);
			} else {
				stub[inputIndex] = ((OngoingStubbing<String>)stub[inputIndex]).thenReturn(line);
			}
			result.add(new Values(new Long(ts), line));
			
			if(r.nextDouble() < 0.7) {
				++ts;
			}
			
		}
		for(int i = 0; i < numberOfInputFiles; ++i) {
			((OngoingStubbing<String>)stub[i]).thenReturn(null);
		}
		
		LinkedList<Integer> validTaskIndex = new LinkedList<Integer>();
		for(int i = 0; i < numberOfTasks; ++i) {
			spoutTasks[i].open(conf, contextMocks[i], new SpoutOutputCollector(collectors[i]));
			validTaskIndex.add(new Integer(i));
		}
		
		int[] invocationCount = new int[numberOfTasks];
		while(validTaskIndex.size() > 0) {
			int validIndex = r.nextInt(validTaskIndex.size());
			int index = validTaskIndex.get(validIndex);
			spoutTasks[index].nextTuple();
			if(++invocationCount[index] == numberOfLines) {
				validTaskIndex.remove(validIndex);
			}
		}
		
		for(List<Object> tuple : result) {
			boolean found = false;
			
			outter: for(int i = 0; i < numberOfTasks; ++i) {
				for(int j = 0; j < collectors[i].output.get(Utils.DEFAULT_STREAM_ID).size(); ++j) {
					if(((Long)collectors[i].output.get(Utils.DEFAULT_STREAM_ID).get(j).get(0)).longValue() > ((Long)tuple
						.get(0)).longValue()) {
						break;
					}
					
					if(collectors[i].output.get(Utils.DEFAULT_STREAM_ID).get(j).equals(tuple)) {
						collectors[i].output.get(Utils.DEFAULT_STREAM_ID).remove(j);
						found = true;
						break outter;
					}
				}
			}
			
			Assert.assertTrue(found);
		}
		for(int i = 0; i < numberOfTasks; ++i) {
			Assert.assertTrue(collectors[i].output.size() == 1);
			Assert.assertTrue(collectors[i].output.get(Utils.DEFAULT_STREAM_ID).size() == 0);
		}
	}
	
	@Test
	public void testDeclareOutputFields() {
		SensorSpout spout = new SensorSpout();
		TestDeclarer declarerMock = new TestDeclarer();
		spout.declareOutputFields(declarerMock);
		
		Assert.assertTrue(declarerMock.schema.size() == 1);
		Assert.assertTrue(declarerMock.streamId.size() == 1);
		Assert.assertTrue(declarerMock.direct.size() == 1);
		Assert.assertEquals(new Fields("ts", "rawTuple").toList(), declarerMock.schema.get(0).toList());
		Assert.assertEquals(null, declarerMock.streamId.get(0));
		Assert.assertEquals(new Boolean(false), declarerMock.direct.get(0));
	}
	
}
