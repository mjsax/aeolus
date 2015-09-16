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

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;





/**
 * {@link FileSinkBolt} writes all received tuples in CSV format to an output file. Each line of the output file consist
 * of a single tuple. If a tuple contains zero attributes, it is represented as {@code NULL} (in capital letters).
 * 
 * @author Matthias J. Sax
 */
public class FileSinkBolt extends AbstractFileOutputBolt {
	private final static long serialVersionUID = -3429907305973973314L;
	
	/** String representation of an {@code null} attribute. */
	private final static String nullAttribute = "null";
	
	/** The name of the output file. */
	private final String outputFileName;
	
	/** The directory of the output file. */
	private final String outputDirName;
	
	public FileSinkBolt(File outputFile) {
		if(!outputFile.isAbsolute()) {
			this.outputFileName = outputFile.getName();
			if(!outputFile.getPath().contains(File.separator)) {
				// simple file without relative path component
				this.outputDirName = null;
			} else {
				this.outputDirName = outputFile.getParentFile().getPath();
			}
		} else {
			this.outputFileName = outputFile.getName();
			this.outputDirName = outputFile.getParentFile().getAbsolutePath();
		}
	}
	
	public FileSinkBolt(String filename) {
		String[] tokens = filename.split(File.separator);
		
		this.outputFileName = tokens[tokens.length - 1];
		
		if(tokens.length > 1) {
			String dir = "";
			for(int i = 0; i < tokens.length - 2; ++i) {
				dir += tokens[i] + File.separator;
			}
			dir += tokens[tokens.length - 2];
			
			this.outputDirName = dir;
		} else {
			this.outputDirName = null;
		}
	}
	
	
	
	@SuppressWarnings("unchecked")
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		HashMap<Object, Object> conf = new HashMap<Object, Object>(stormConf);
		String fileName = (String)conf.get(AbstractFileOutputBolt.OUTPUT_FILE_NAME);
		if(fileName == null) {
			conf.put(AbstractFileOutputBolt.OUTPUT_FILE_NAME, this.outputFileName);
		}
		
		String dirName = (String)conf.get(AbstractFileOutputBolt.OUTPUT_DIR_NAME);
		if(dirName == null && this.outputDirName != null) {
			conf.put(AbstractFileOutputBolt.OUTPUT_DIR_NAME, this.outputDirName);
		}
		
		super.prepare(conf, context, collector);
	}
	
	@Override
	public String tupleToString(Tuple input) {
		final int numberOfAttributes = input.size();
		if(numberOfAttributes > 0) {
			Object attribute = input.getValue(0);
			if(attribute == null) {
				attribute = nullAttribute;
			}
			String record = attribute.toString();
			
			for(int i = 1; i < numberOfAttributes; ++i) {
				attribute = input.getValue(i);
				if(attribute == null) {
					attribute = nullAttribute;
				}
				
				record += "," + attribute.toString();
			}
			
			return record + "\n";
		}
		
		return "NULL\n";
	}
	
}
