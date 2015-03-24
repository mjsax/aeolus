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
package de.hub.cs.dbis.aeolus.monitoring.utils;

import org.junit.Assert;
import org.junit.Test;

import de.hub.cs.dbis.aeolus.monitoring.utils.AeolusConfig;





/**
 * @author Matthias J. Sax
 */
public class AeolusConfigTest {
	
	@Test
	public void testNimbusHost() {
		AeolusConfig config = new AeolusConfig();
		String hostName = "dummy";
		config.config.put(AeolusConfig.NIMBUS_HOST, hostName);
		
		Assert.assertEquals(hostName, config.getNimbusHost());
	}
	
	@Test
	public void testNimbusPort() {
		AeolusConfig config = new AeolusConfig();
		Integer portNumber = new Integer(42);
		config.config.put(AeolusConfig.NIMBUS_PORT, portNumber);
		
		Assert.assertEquals(portNumber, config.getNimbusPort());
	}
	
	@Test
	public void testNimbusPortNumber() {
		AeolusConfig config = new AeolusConfig();
		int portNumber = 42;
		config.config.put(AeolusConfig.NIMBUS_PORT, new Integer(portNumber));
		
		Assert.assertEquals(portNumber, config.getNimbusPortValue());
	}
	
}
