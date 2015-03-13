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
package de.hub.cs.dbis.aeolus.batching;

import static org.mockito.Mockito.mock;

import java.util.Random;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import backtype.storm.topology.IRichSpout;





/**
 * @author Matthias J. Sax
 */
public class SpoutOutputBatcherTest {
	private static IRichSpout spoutMockStatic;
	
	private long seed;
	private Random r;
	
	@BeforeClass
	public static void prepareStatic() {
		spoutMockStatic = mock(IRichSpout.class);
	}
	
	@Before
	public void prepare() {
		this.seed = System.currentTimeMillis();
		this.r = new Random(this.seed);
		System.out.println("Test seed: " + this.seed);
		
	}
	
	@Test
	public void testOpen() {
		SpoutOutputBatcher spout = new SpoutOutputBatcher(spoutMockStatic, 10);
		
		// spout.open(null, null, null);
	}
	
	@Test
	public void testClose() {
		
	}
	
	@Test
	public void testActivate() {
		
	}
	
	@Test
	public void testDeactivate() {
		
	}
	
	@Test
	public void testAck() {
		
	}
	
	@Test
	public void testFail() {
		
	}
	
	@Test
	public void testDeclareOutputFields() {
		
	}
	
	@Test
	public void testGetComponentConfiguration() {}
	
}
