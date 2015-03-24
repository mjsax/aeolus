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

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import de.hub.cs.dbis.aeolus.monitoring.utils.AeolusConfig;
import de.hub.cs.dbis.aeolus.monitoring.utils.ConfigReader;





/**
 * @author Matthias J. Sax
 */
public class ConfigReaderTest {
	
	@Test
	public void testDefault() throws IOException {
		AeolusConfig config = ConfigReader.readConfig();
		
		Assert.assertEquals("default-dummy", config.getNimbusHost());
		Assert.assertEquals(new Integer(0), config.getNimbusPort());
		Assert.assertEquals(0, config.getNimbusPortValue());
	}
	
	@Test
	public void testFileName() throws IOException {
		AeolusConfig config = ConfigReader.readConfig("src/test/resources/dummy.conf");
		
		Assert.assertEquals("non-default-dummy", config.getNimbusHost());
		Assert.assertEquals(new Integer(42), config.getNimbusPort());
		Assert.assertEquals(42, config.getNimbusPortValue());
	}
	
	@Test
	public void testDirName1() throws IOException {
		AeolusConfig config = ConfigReader.readConfig("src/test/resources");
		
		Assert.assertEquals("non-default-dummy-42", config.getNimbusHost());
		Assert.assertEquals(new Integer(4242), config.getNimbusPort());
		Assert.assertEquals(4242, config.getNimbusPortValue());
	}
	
	@Test
	public void testDirName2() throws IOException {
		AeolusConfig config = ConfigReader.readConfig("src/test/resources/");
		
		Assert.assertEquals("non-default-dummy-42", config.getNimbusHost());
		Assert.assertEquals(new Integer(4242), config.getNimbusPort());
		Assert.assertEquals(4242, config.getNimbusPortValue());
	}
	
}
