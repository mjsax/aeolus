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
package de.hub.cs.dbis.aeolus.sinks;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileWriter;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import de.hub.cs.dbis.aeolus.sinks.FileSinkBolt;
import backtype.storm.Config;
import backtype.storm.tuple.Tuple;
import static org.mockito.Mockito.times;





/**
 * @author Matthias J. Sax
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(FileSinkBolt.class)
public class FileSinkBoltTest {
	
	
	
	@Before
	public void prepare() throws Exception {
		PowerMockito.whenNew(FileWriter.class).withAnyArguments().thenReturn(PowerMockito.mock(FileWriter.class));
	}
	
	
	
	@Test
	public void testConstructorSimpleFileName() throws Exception {
		String filename = "simple.file";
		
		FileSinkBolt bolt = new FileSinkBolt(filename);
		
		Config stormConf = new Config();
		bolt.prepare(stormConf, null, null);
		
		String expectedArgument = "." + File.separator + filename;
		bolt = new FileSinkBolt(new File(filename));
		bolt.prepare(stormConf, null, null);
		PowerMockito.verifyNew(FileWriter.class, times(2)).withArguments(expectedArgument);
	}
	
	@Test
	public void testConstructorRelativePath() throws Exception {
		String file = "simple.file";
		String dir = ".." + File.separator + "directory1" + File.separator + "directory2";
		String filename = dir + File.separator + file;
		
		FileSinkBolt bolt = new FileSinkBolt(filename);
		
		Config stormConf = new Config();
		bolt.prepare(stormConf, null, null);
		
		bolt = new FileSinkBolt(new File(filename));
		bolt.prepare(stormConf, null, null);
		PowerMockito.verifyNew(FileWriter.class, times(2)).withArguments(filename);
	}
	
	@Test
	public void testConstructorAbsolutePath() throws Exception {
		String file = "simple.file";
		String dir = File.separator + "directory1" + File.separator + "directory2";
		String filename = dir + File.separator + file;
		
		FileSinkBolt bolt = new FileSinkBolt(filename);
		
		Config stormConf = new Config();
		bolt.prepare(stormConf, null, null);
		
		bolt = new FileSinkBolt(new File(filename));
		bolt.prepare(stormConf, null, null);
		PowerMockito.verifyNew(FileWriter.class, times(2)).withArguments(filename);
	}
	
	@Test
	public void testTupleToString() {
		FileSinkBolt bolt = new FileSinkBolt("");
		
		Tuple t = mock(Tuple.class);
		
		when(new Integer(t.size())).thenReturn(new Integer(0));
		Assert.assertEquals("NULL\n", bolt.tupleToString(t));
		
		when(new Integer(t.size())).thenReturn(new Integer(1));
		when(t.getValue(0)).thenReturn(null);
		Assert.assertEquals("null\n", bolt.tupleToString(t));
		
		when(new Integer(t.size())).thenReturn(new Integer(1));
		when(t.getValue(0)).thenReturn("attribute");
		Assert.assertEquals("attribute\n", bolt.tupleToString(t));
		
		when(new Integer(t.size())).thenReturn(new Integer(2));
		when(t.getValue(0)).thenReturn("attribute1");
		when(t.getValue(1)).thenReturn("attribute2");
		Assert.assertEquals("attribute1,attribute2\n", bolt.tupleToString(t));
		
		when(new Integer(t.size())).thenReturn(new Integer(4));
		when(t.getValue(0)).thenReturn(new Integer(4));
		when(t.getValue(1)).thenReturn("attribute2");
		when(t.getValue(2)).thenReturn(null);
		when(t.getValue(3)).thenReturn(new Character('m'));
		Assert.assertEquals("4,attribute2,null,m\n", bolt.tupleToString(t));
	}
	
}
