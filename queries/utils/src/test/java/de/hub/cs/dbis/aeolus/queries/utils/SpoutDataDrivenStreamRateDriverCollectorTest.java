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
package de.hub.cs.dbis.aeolus.queries.utils;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.Random;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;

import backtype.storm.spout.SpoutOutputCollector;





/**
 * @author Matthias J. Sax
 */
@RunWith(PowerMockRunner.class)
public class SpoutDataDrivenStreamRateDriverCollectorTest {
	private long seed;
	private Random r;
	
	
	
	@Before
	public void prepare() {
		this.seed = System.currentTimeMillis();
		this.r = new Random(this.seed);
		System.out.println("Test seed: " + this.seed);
	}
	
	
	
	@Test
	public void testCollector() {
		final int numberOfAttributes = 10;
		int index = (int)(this.r.nextDouble() * numberOfAttributes);
		
		SpoutOutputCollector collector = mock(SpoutOutputCollector.class);
		SpoutDataDrivenStreamRateDriverCollector<Long> driverCollector = new SpoutDataDrivenStreamRateDriverCollector<Long>(
			collector, index);
		
		ArrayList<Object> tuple = new ArrayList<Object>(numberOfAttributes);
		for(int i = 0; i < numberOfAttributes; ++i) {
			tuple.add(new Long(i));
		}
		Object messageId = mock(Object.class);
		String streamId = "streamId";
		
		
		
		driverCollector.emit(tuple);
		verify(collector).emit(tuple);
		assert (driverCollector.timestampLastTuple == index);
		
		driverCollector.emit(tuple, messageId);
		verify(collector).emit(tuple, messageId);
		assert (driverCollector.timestampLastTuple == index);
		
		driverCollector.emit(streamId, tuple);
		verify(collector).emit(streamId, tuple);
		assert (driverCollector.timestampLastTuple == index);
		
		driverCollector.emit(streamId, tuple, messageId);
		verify(collector).emit(streamId, tuple, messageId);
		assert (driverCollector.timestampLastTuple == index);
		
		int taskId = this.r.nextInt();
		driverCollector.emitDirect(taskId, tuple);
		verify(collector).emitDirect(taskId, tuple);
		assert (driverCollector.timestampLastTuple == index);
		taskId = this.r.nextInt();
		driverCollector.emitDirect(taskId, tuple);
		verify(collector).emitDirect(taskId, tuple);
		assert (driverCollector.timestampLastTuple == index);
		
		taskId = this.r.nextInt();
		driverCollector.emitDirect(taskId, tuple, messageId);
		verify(collector).emitDirect(taskId, tuple, messageId);
		assert (driverCollector.timestampLastTuple == index);
		taskId = this.r.nextInt();
		driverCollector.emitDirect(taskId, tuple, messageId);
		verify(collector).emitDirect(taskId, tuple, messageId);
		assert (driverCollector.timestampLastTuple == index);
		
		taskId = this.r.nextInt();
		driverCollector.emitDirect(taskId, streamId, tuple);
		verify(collector).emitDirect(taskId, streamId, tuple);
		assert (driverCollector.timestampLastTuple == index);
		taskId = this.r.nextInt();
		driverCollector.emitDirect(taskId, streamId, tuple);
		verify(collector).emitDirect(taskId, streamId, tuple);
		assert (driverCollector.timestampLastTuple == index);
		
		taskId = this.r.nextInt();
		driverCollector.emitDirect(taskId, streamId, tuple, messageId);
		verify(collector).emitDirect(taskId, streamId, tuple, messageId);
		assert (driverCollector.timestampLastTuple == index);
		taskId = this.r.nextInt();
		driverCollector.emitDirect(taskId, streamId, tuple, messageId);
		verify(collector).emitDirect(taskId, streamId, tuple, messageId);
		assert (driverCollector.timestampLastTuple == index);
		
		Throwable error = mock(Throwable.class);
		driverCollector.reportError(error);
		verify(collector).reportError(error);
		
	}
	
	@Test
	public void testCollectorInt() {
		final int numberOfAttributes = 10;
		int index = this.r.nextInt(numberOfAttributes);
		
		SpoutOutputCollector collector = mock(SpoutOutputCollector.class);
		SpoutDataDrivenStreamRateDriverCollector<Integer> driverCollector = new SpoutDataDrivenStreamRateDriverCollector<Integer>(
			collector, index);
		
		ArrayList<Object> tuple = new ArrayList<Object>(numberOfAttributes);
		for(int i = 0; i < numberOfAttributes; ++i) {
			tuple.add(new Integer(i));
		}
		
		driverCollector.emit(tuple);
		verify(collector).emit(tuple);
		assert (driverCollector.timestampLastTuple == index);
	}
	
}
