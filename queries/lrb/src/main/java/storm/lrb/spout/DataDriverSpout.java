package storm.lrb.spout;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.log4j.Logger;

import storm.lrb.tools.StopWatch;

/**
 * This is a quick fix trying to overcome limitations of the actual datadriver
 * by reading directly from file and emiting to topology this is not functioning
 * yet and was mainly not finished because no distributed filesystem was
 * available (at short notice)..
 * 
 */
@SuppressWarnings("serial")
public class DataDriverSpout extends BaseRichSpout {
	private static final long serialVersionUID = 1L;
	SpoutOutputCollector _collector;
	BufferedReader _bufferedReader;

	// int offset;
	long tupleCnt = 0;

	public static ServerSocket server;
	public static int bufferInSeconds = 10; // (+1)
	public static int readSecond = 0; // runner second for reading
	public static int sendSecond = 0; // runner second for sending
	public static int queueBuffer = 10; // buffertime till server stops, if no
										// client sends a "GET"
	private static boolean firstStart = true; // to set offset
	public static BufferedReader in;
	public static int offset = 0;
	
	private String filename; 
	
	public static Map<Integer, Queue<String>> bufferlist = new HashMap<Integer, Queue<String>>();
		
	public static StopWatch stw = new StopWatch();
	
	public static PrintStream socketOutput;

	private boolean local = false;
	private boolean firstrun;
	
	public FillBufferQueue bufferer;
	
	public static Timer timer;

	private static final Logger LOG = Logger.getLogger(DataDriverSpout.class);

	/**
	 * DataDriverKonstruktor
	 * @param filename of input data
	 */
	public DataDriverSpout(String file) {
		if(file.isEmpty()) throw new Error("No simulation input file given");
		this.filename = file;
	}

	@Override
	public void open(Map conf, TopologyContext context,SpoutOutputCollector collector) {
		_collector = collector;
		try {
			FileInputStream fileinput;

			fileinput = new FileInputStream(filename);

			in = new BufferedReader(new InputStreamReader(fileinput));

			if (initQueue()) {
				System.out.println("Queue successfully initialized with a buffer of "
								+ bufferInSeconds + " seconds.");
				System.out.println("Bufferlist: " + bufferlist.get(0).toString());
			}

			stw.start(DataDriverSpout.offset);
		
			// Timer to refill Queue each second
			
			Thread t = new Thread(new Runnable() {
		        public void run() {
		        		timer = new Timer();
		        		timer.schedule(new FillBufferQueue(),2000, 1000);
		        	}
			});
			t.start();
			
		

			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				
			}

	}

	
	
	
	
	@Override
	public void nextTuple() {
		
	try {

		if ((sendSecond <= stw.getElapsedTimeSecs())) {
			Queue<String> myQ = bufferlist.get(sendSecond);
			
			while (myQ!=null && myQ.size()>0) {
				if(tupleCnt%100==0) System.out.println("DATADRIVER:: " + myQ.peek() + " at" + stw.getElapsedTimeSecs());
				_collector.emit("stream", new Values(myQ.poll(), stw));//,tupleCnt);
				tupleCnt++;
			}
			System.out.println("queue empty wait for next second?"+ bufferlist.keySet());
			//queue empty go to next second
			sendSecond++;
		}else
			Utils.sleep((sendSecond-stw.getElapsedTimeSecs())*100);
			System.out.println("DDS: waiting for next second?");
		
	} catch (IndexOutOfBoundsException ie) {
		//System.out.println("No more lines in Queue, Sever stopped!");
		System.out.println("BufferSize: " + bufferlist.keySet() + " SendSecond " + sendSecond );
		ie.printStackTrace();
	} catch (Exception e) {
		e.printStackTrace();
	}

	}

	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("stream", new Fields("tuple", "StormTimer"));
		// declarer.declareStream("stormtimer", new Fields("StormStartTime",
		// "StartOffset"));

	}
	
/*
	@Override
	public Map<String, Object> getComponentConfiguration() {
		Config conf = new Config();
		conf.setMaxTaskParallelism(3);
		int tickFrequencyInSeconds = 6;
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, tickFrequencyInSeconds);
		return conf;
	}
	*/
	/**
	 * Inits the Queue at the first start
	 * 
	 * @return If initialization was successful
	 */
	private static boolean initQueue() {
		String newSecondfirst = "";
		try {
			Queue<String> myQ = new LinkedList<String>();
			// Reader ready? && actual read second < queueBuffer?
			while (in.ready() && readSecond <= queueBuffer + stw.getElapsedTimeSecs()) {
				String line = in.readLine();
				
				line = transformTuple(line);
				String tupel[] = line.split(",");
				
				if(firstStart == true) {
					firstStart = false;
					offset = Integer.parseInt(tupel[1]);
					if (offset > 0) {
						System.out.println("Simulation starts with offset of: "+ offset);
						stw.setOffset(offset);
					}
				}
				
				//add second from last round
				while (newSecondfirst != "") {
					myQ.add(newSecondfirst);
					newSecondfirst = "";
				}
				// Same second
				if (Integer.parseInt(tupel[1]) == (offset + readSecond)) {
					myQ.add(line);
				}else { // new second, prepare last tupel of new second
					newSecondfirst = line;
					bufferlist.put(readSecond,myQ);
					readSecond++;
					myQ = new LinkedList<String>();
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		
		
		return true;
	}

	public static String transformTuple(String line) {
		if(line==null) return "";
		if(line.startsWith("#(")){
			line = line.replace("#(", "");
			line = line.replace(")", "");
			line = line.replaceAll(" ", ",");
		}
		return line;
	}

}
class FillBufferQueue extends TimerTask {
	int actualSecond = 0;
	String newSecondfirst = "";
	int lastSecDeleted = -1;
	int test;
	StopWatch checktime = new StopWatch();

	// Filling up the Queue, for x seconds in the future (queueBuffer
	@Override
	public void run() {
		checktime.start();
	
		
	try {
		
		while(!DataDriverSpout.stw.running){
		 System.out.println("FillBufferQueue sleeping");	Utils.sleep(1000);
		}
		
		System.out.println("Sim starting fill bufferqueue at::" + DataDriverSpout.stw);
		//test = (int) (DataDriverSpout.stw.getElapsedTimeSecs() + DataDriverSpout.bufferInSeconds);
		
		Queue<String> myQ = new LinkedList<String>();
			
		if (DataDriverSpout.in.ready() && DataDriverSpout.stw.running) {// && TCPDataFeeder2.readSecond <=
			int test;							// TCPDataFeeder2.stw.getElapsedTimeSecs()){
			if (DataDriverSpout.stw.getElapsedTimeSecs() > 0 && (test = (DataDriverSpout.sendSecond + DataDriverSpout.offset)) < DataDriverSpout.stw.getElapsedTimeSecs()+20) {
				System.out.println("Too slow with output at "
								+ DataDriverSpout.stw.getElapsedTimeSecs() + "while sendsecond is"+test);
				 //System.out.println("FillBufferQueue sleeping");	Utils.sleep(1000);
				
				
			} else {

				//delete old stuff
				for (int i = lastSecDeleted; i < DataDriverSpout.sendSecond; i++) {
					if(DataDriverSpout.bufferlist.containsKey(i) &&
							DataDriverSpout.bufferlist.get(i).isEmpty()){
						DataDriverSpout.bufferlist.remove(i);
						lastSecDeleted = i;
					}
				}
				
				//fill it up so that we have at least a buffer of 5 seconds
				while (DataDriverSpout.in.ready() && DataDriverSpout.readSecond <= DataDriverSpout.queueBuffer + DataDriverSpout.stw.getElapsedTimeSecs()) {
					String line = DataDriverSpout.in.readLine();
					if (line != null) {
						//in case the input file is fro uppsalla
						line = DataDriverSpout.transformTuple(line);
						String tupel[] = line.split(",");
						// Add last tupel of new second
						while (newSecondfirst != "") {
							myQ.add(newSecondfirst);
							newSecondfirst = "";
						}
						
						// Same second
						if (Integer.parseInt(tupel[1]) == (DataDriverSpout.offset + DataDriverSpout.readSecond)) {
							myQ.add(line);
						 System.out.println("added tuples for minute:"+DataDriverSpout.readSecond );
						}else { // new second, prepare last tupel of new second

							newSecondfirst = line;
							DataDriverSpout.bufferlist.put(DataDriverSpout.readSecond,myQ);
							DataDriverSpout.readSecond++;
							myQ = new LinkedList<String>();
						
						}
					}else {
						int cursec = DataDriverSpout.offset + DataDriverSpout.readSecond;
						//System.out.println("Last Line Added! at"+cursec);
						this.cancel(); //Not necessary because we call System.exit
			            //System.exit(0);
						}
					}
				}
			}
	
		System.out.println(checktime.getDurationTimeSecs()+" to bufferup");
		
				
		
	} catch (Exception e) {
		e.printStackTrace();
	}
	}


		
	}


