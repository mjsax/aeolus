package de.hub.cs.dbis.aeolus.queries.utils;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
		when(context.getComponentOutputFields(null, null)).thenReturn(new Fields("dummy"));
		
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
