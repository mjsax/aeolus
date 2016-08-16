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
package de.hub.cs.dbis.aeolus.monitoring.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;





/**
 * {@link ConfigReader} reads an Aeolus configuration from a plain text file.
 * 
 * @author mjsax
 */
public class ConfigReader {
	private final static Logger logger = LoggerFactory.getLogger(ConfigReader.class);
	
	/** The default configuration file name. */
	public final static String defaultConfigFile = "aeolus.conf";
	
	
	
	/**
	 * Reads an Aeolus configuration from the default configuration file {@link #defaultConfigFile}.
	 * 
	 * @return a configuration object populated with the specified values from the configuration file
	 * 
	 * @throws IOException
	 *             if the configuration file could not be processed successfully
	 */
	public static AeolusConfig readConfig() throws IOException {
		return readConfig(defaultConfigFile);
	}
	
	/**
	 * Reads an Aeolus configuration from the specified configuration file.
	 * 
	 * @return a configuration object populated with the specified values from the configuration file
	 * 
	 * @throws IOException
	 *             if the configuration file could not be processed successfully
	 */
	public static AeolusConfig readConfig(String configFile) throws IOException {
		AeolusConfig config = new AeolusConfig();
		
		if(new File(configFile).isDirectory()) {
			if(!configFile.endsWith(File.separator)) {
				configFile += File.separator;
			}
			configFile += defaultConfigFile;
		}
		
		BufferedReader reader = new BufferedReader(new FileReader(configFile));
		String line = null;
		int lineNumber = 0;
		while(true) {
			try {
				line = reader.readLine();
				if(line == null) {
					break;
				}
				++lineNumber;
				
				if(line.trim().length() == 0 || line.startsWith("#")) {
					continue;
				}
				
				String[] token = line.split("=");
				if(token.length != 2) {
					logger.warn("Skipping invalid line #{}.", new Integer(lineNumber));
					continue;
				}
				
				if(token[0].equals(AeolusConfig.NIMBUS_HOST)) {
					config.config.put(AeolusConfig.NIMBUS_HOST, token[1]);
				} else if(token[0].equals(AeolusConfig.NIMBUS_PORT)) {
					try {
						config.config.put(AeolusConfig.NIMBUS_PORT, new Integer(Integer.parseInt(token[1])));
					} catch(NumberFormatException e) {
						logger.warn("Skipping line #{}. Could not pares port number '{}'.", new Integer(lineNumber),
							token[1]);
					}
				} else {
					logger.warn("Skipping line #{} due to unknown key '{}'.", new Integer(lineNumber), token[0]);
				}
			} catch(IOException e) {
				logger.warn("IOException occured reading line #{}.", new Integer(lineNumber + 1));
				reader.close();
				throw e;
			}
		}
		reader.close();
		
		return config;
	}
	
}
