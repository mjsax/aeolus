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
package de.hub.cs.dbis.aeolus.utils;

import java.util.HashMap;





/**
 * {@link AeolusConfig} is a universal configuration object for the whole optimization package.
 * 
 * @author Matthias J. Sax
 */
public class AeolusConfig {
	public final static String NIMBUS_HOST = "nimbus.host";
	public final static String NIMBUS_PORT = "nimbus.port";
	
	/**
	 * Contains the actual configuration key-value-pairs.
	 */
	final HashMap<String, Object> config = new HashMap<String, Object>();
	
	public String getNimbusHost() {
		return (String)this.config.get(NIMBUS_HOST);
	}
	
	public Integer getNimbusPort() {
		return (Integer)this.config.get(NIMBUS_PORT);
		
	}
	
	public int getNimbusPortValue() {
		return ((Integer)this.config.get(NIMBUS_PORT)).intValue();
		
	}
	
}
