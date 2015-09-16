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
package de.hub.cs.dbis.aeolus.testUtils;

import java.util.Random;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;





/**
 * 
 * @author richter
 */
public class AbstractBoltTest {
	protected Random r;
	protected long seed;
	private final static Logger LOGGER = LoggerFactory.getLogger(AbstractBoltTest.class);
	
	@Before
	public void prepare() {
		this.seed = System.currentTimeMillis();
		this.r = new Random(this.seed);
		LOGGER.info("Test seed: {}", this.seed);
	}
}
