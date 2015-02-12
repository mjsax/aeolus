package storm.lrb.bolt;

/*
 * #%L
 * lrb
 * %%
 * Copyright (C) 2014 - 2015 Humboldt-Universität zu Berlin
 * %%
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
 * #L%
 */

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

import org.joda.time.DateTime;

import storm.lrb.tools.TupleHelpers;

/**
 * This bolt bufferwrites all recieved tuples to the given filename in the STORM_LOCAL_DIR.
 * The buffersize can be adjusted with the constructor.
 * (flushes every two minutes, this can be adjusted by changing the TOPOLOGY_TICK_TUPLE_FREQ_SECS)
 */
public class FileWriterBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;

	PrintWriter printwriter;
	Writer filewriter;
	Writer bufferedwriter;

	int count = 0;
	private OutputCollector _collector;

	private String filename;
	
	private final boolean local;

	private int bufferfactor = 2;

	public FileWriterBolt(String filename, int bufferfactor) {
		// TODO Auto-generated constructor stub
		this.filename = filename;
		this.local = false;
		this.bufferfactor = bufferfactor;
	} 
	public FileWriterBolt(String filename, boolean submitted) {
		this.filename = filename;
		this.local = !submitted;
	}

	/**
	 * set the bufferfactor higher if a the rate of emitting tuples is expected to be high
	 */
	public FileWriterBolt(String filename, int bufferfactor, boolean submitted) {
		
		this.filename = filename;
		this.local = submitted;
		this.bufferfactor = bufferfactor;
	}

	@Override
	public void prepare(Map conf, TopologyContext topologyContext,
			OutputCollector outputCollector) {
		_collector = outputCollector;
		printwriter = null;

		String path = (String) conf.get(Config.STORM_LOCAL_DIR);

		DateTime dt = new DateTime();
		String b = dt.toString("hh-mm-ss");

		String fileuri = path + "/" + filename+"_" +b+  ".out";
		System.out.println("Writing to : " + fileuri);

		try {

			filewriter = new FileWriter(fileuri);
			bufferedwriter = new BufferedWriter(filewriter, bufferfactor * 1024);
			printwriter = new PrintWriter(bufferedwriter);

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@Override
	public void execute(Tuple tuple) {

		if (TupleHelpers.isTickTuple(tuple)) {
			
			printwriter.flush();
			
			return;
		} else {
			printwriter.println(tuple.getValue(0));
			_collector.ack(tuple);
			
			if(local) printwriter.flush();
			
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Map<String, Object> conf = new HashMap<String, Object>();
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 70);
		return conf;
	}

	@Override
	public void cleanup() {
		printwriter.flush();
		printwriter.close();
		try {
			bufferedwriter.close();
			filewriter.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		super.cleanup();

	}
}