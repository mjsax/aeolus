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
package de.hub.cs.dbis.lrb.operators;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import de.hub.cs.dbis.aeolus.testUtils.TestDeclarer;
import de.hub.cs.dbis.aeolus.testUtils.TestSpoutOutputCollector;
import de.hub.cs.dbis.aeolus.utils.TimestampMerger;





/**
 * @author mjsax
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(FileReaderSpout.class)
public class FileReaderSpoutTest {
	
	@Test
	public void test() throws Exception {
		FileReader fileReaderMock = PowerMockito.mock(FileReader.class);
		PowerMockito.whenNew(FileReader.class).withArguments("xway").thenReturn(fileReaderMock);
		
		BufferedReader bufferedReaderMock = PowerMockito.mock(BufferedReader.class);
		PowerMockito.whenNew(BufferedReader.class).withArguments(fileReaderMock).thenReturn(bufferedReaderMock);
		
		final String line = "type,73647,dummy-attributes";
		when(bufferedReaderMock.readLine()).thenReturn(line).thenReturn(null);
		
		FileReaderSpout spout = new FileReaderSpout();
		
		List<Integer> taskMock = new LinkedList<Integer>();
		taskMock.add(new Integer(0));
		TopologyContext contextMock = mock(TopologyContext.class);
		when(contextMock.getComponentTasks(anyString())).thenReturn(taskMock);
		when(new Integer(contextMock.getThisTaskIndex())).thenReturn(new Integer(0));
		
		HashMap<Object, Object> dummyConf = new HashMap<Object, Object>();
		TestSpoutOutputCollector collector = new TestSpoutOutputCollector();
		spout.open(dummyConf, contextMock, new SpoutOutputCollector(collector));
		
		spout.nextTuple();
		spout.nextTuple();
		spout.nextTuple();
		
		Assert.assertEquals(1, collector.output.size());
		Assert.assertEquals(new Values(new Long(73647), line), collector.output.get(Utils.DEFAULT_STREAM_ID)
			.removeFirst());
		Assert.assertEquals(0, collector.output.get(Utils.DEFAULT_STREAM_ID).size());
		
		Assert.assertNull(collector.output.get(TimestampMerger.FLUSH_STREAM_ID));
		
		spout.deactivate();
		
		Assert.assertEquals(1, collector.output.get(TimestampMerger.FLUSH_STREAM_ID).size());
		Assert.assertEquals(new Values((Object)null), collector.output.get(TimestampMerger.FLUSH_STREAM_ID).get(0));
	}
	
	@Test
	public void testDeclareOutputFields() {
		FileReaderSpout spout = new FileReaderSpout();
		
		TestDeclarer declarer = new TestDeclarer();
		spout.declareOutputFields(declarer);
		
		final int numberOfOutputStreams = 2;
		
		Assert.assertEquals(numberOfOutputStreams, declarer.streamIdBuffer.size());
		Assert.assertEquals(numberOfOutputStreams, declarer.schemaBuffer.size());
		Assert.assertEquals(numberOfOutputStreams, declarer.directBuffer.size());
		
		HashMap<String, Fields> expectedStreams = new HashMap<String, Fields>();
		expectedStreams.put(null, new Fields("ts", "rawTuple"));
		expectedStreams.put(TimestampMerger.FLUSH_STREAM_ID, new Fields("ts"));
		
		Assert.assertEquals(expectedStreams.keySet(), new HashSet<String>(declarer.streamIdBuffer));
		
		for(int i = 0; i < numberOfOutputStreams; ++i) {
			Assert.assertEquals(expectedStreams.get(declarer.streamIdBuffer.get(i)).toList(), declarer.schemaBuffer
				.get(i).toList());
			Assert.assertEquals(new Boolean(false), declarer.directBuffer.get(i));
		}
	}
	
}
