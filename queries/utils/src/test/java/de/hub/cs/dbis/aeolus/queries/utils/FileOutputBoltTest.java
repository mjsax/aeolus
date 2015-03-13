package de.hub.cs.dbis.aeolus.queries.utils;

/*
 * #%L
 * utils
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


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.LinkedList;
import java.util.Random;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import backtype.storm.Config;
import backtype.storm.task.GeneralTopologyContext;
import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImpl;
import backtype.storm.tuple.Values;
import de.hub.cs.dbis.aeolus.testUtils.TestOutputCollector;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;





/**
 * @author Matthias J. Sax
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(TestFileOutputBolt.class)
public class FileOutputBoltTest {
	
	private long seed;
	private Random r;
	
	
	@Before
	public void prepare() {
		this.seed = System.currentTimeMillis();
		this.r = new Random(this.seed);
		System.out.println("Test seed: " + this.seed);
	}
	
	
	
	@Test
	public void testExecute() throws Exception {
		final LinkedList<String> expectedResult = new LinkedList<String>();
		final LinkedList<String> result = new LinkedList<String>();
		final LinkedList<Tuple> input = new LinkedList<Tuple>();
		
		
		Config conf = new Config();
		String dummyDir = "dummyDir";
		String dummyFile = "dummyFile";
		
		String usedDir = ".";
		String usedFile = "result.dat";
		switch(this.r.nextInt(4)) {
		case 0:
			conf.put(TestFileOutputBolt.OUTPUT_DIR_NAME, dummyDir);
			usedDir = dummyDir;
			break;
		case 1:
			conf.put(TestFileOutputBolt.OUTPUT_FILE_NAME, dummyFile);
			usedFile = dummyFile;
			break;
		case 2:
			conf.put(TestFileOutputBolt.OUTPUT_DIR_NAME, dummyDir);
			conf.put(TestFileOutputBolt.OUTPUT_FILE_NAME, dummyFile);
			usedDir = dummyDir;
			usedFile = dummyFile;
			break;
		default:
		}
		
		FileWriter fileWriterMock = PowerMockito.mock(FileWriter.class);
		PowerMockito.whenNew(FileWriter.class).withArguments(usedDir + File.separator + usedFile)
			.thenReturn(fileWriterMock);
		
		BufferedWriter dummyWriter = new BufferedWriter(fileWriterMock) {
			@Override
			public void write(String s) {
				result.add(s);
			}
		};
		PowerMockito.whenNew(BufferedWriter.class).withArguments(fileWriterMock).thenReturn(dummyWriter);
		
		
		TestFileOutputBolt bolt = new TestFileOutputBolt();
		TestOutputCollector collector = new TestOutputCollector();
		bolt.prepare(conf, null, new OutputCollector(collector));
		
		GeneralTopologyContext context = mock(GeneralTopologyContext.class);
		when(context.getComponentOutputFields(anyString(), anyString())).thenReturn(new Fields("dummy"));
		when(context.getComponentId(anyInt())).thenReturn("componentID");
		
		final int numberOfLines = 20;
		for(int i = 0; i < numberOfLines; ++i) {
			TupleImpl t = new TupleImpl(context, new Values(new Integer(this.r.nextInt())), 0, null);
			input.add(t);
			expectedResult.add(t.toString());
			bolt.execute(t);
		}
		
		Assert.assertEquals(expectedResult, result);
		Assert.assertEquals(input, collector.acked);
	}
	
}
