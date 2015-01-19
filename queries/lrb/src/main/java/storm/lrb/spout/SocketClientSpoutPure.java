package storm.lrb.spout;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import storm.lrb.tools.StopWatch;

/**
 * This Spout connects to the given host and socket, reads tuples line by line
 * and emits it to the default stream. This spout is used to make reading from socket faster
 * by defering LRBtuple creation to the {@link DispatcherBolt} which can be parallelized
 * 
 */
@SuppressWarnings("serial")
public class SocketClientSpoutPure extends BaseRichSpout {
	SpoutOutputCollector _collector;
	LinkedBlockingQueue<String> queue = null;
	String _host;
	int _port;
	Socket clientSocket;
	InputStreamReader isr;
	BufferedReader in;
	StopWatch cnt;
	int _processed_xway;
	long tupleCnt = 0;
	boolean firstrun = true;
	
	

	private static final Logger LOG = Logger.getLogger(SocketClientSpoutPure.class);

	public SocketClientSpoutPure(String host, int port) {
		_host = host;
		_port = port;
		this.cnt = new StopWatch();
		
	
	}

	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf,TopologyContext context, SpoutOutputCollector collector) {
		
		_collector = collector;

		boolean goon = true;
		//try no more than 2 minutes to connect 
		while (cnt.getElapsedTimeSecs()  <= 120) {
			try {
				
				if (cnt.getElapsedTimeSecs() > 2)	Utils.sleep(5000);

				if (goon == true) 	goon = false;
				else				break;
				
				clientSocket = new Socket(InetAddress.getByName(_host), _port);
				LOG.info("Connection to " + _host + ":" + _port
						+ " established.\t StormTimer: "+ cnt.getElapsedTimeSecs()+"s");
				System.out.println("######connection established");
			
				isr = new InputStreamReader(clientSocket.getInputStream());
				in = new BufferedReader(isr);

			} catch (java.net.ConnectException e) {
				goon = true;
				LOG.info("Trying to connect...");
				System.out.println("Trying to connect...");
			} catch (UnknownHostException e) {
				e.printStackTrace();
				LOG.error("Unknown Host...",e);
			} catch (IOException e) {
				e.printStackTrace();
				LOG.error("IOexception...",e);
			}
		}

	}

	@Override
	public void nextTuple() {

		String line = "";

		try {
			if ((line = in.readLine()) != null) {
				if (line.startsWith("#")) {
                                    return;
                                }	
				if(firstrun){
					int offset = Integer.parseInt(line.substring(2, line.indexOf(",", 2)));
					LOG.info("Simulation starts with offset "+offset);
					cnt.start(offset);
					firstrun = false;
				}
				//LOG.info(line+ " at "+cnt.getElapsedTimeSecs());
				_collector.emit("stream", new Values(line,cnt), tupleCnt);
				tupleCnt++;
				
			}else if (line == null)
				Utils.sleep(50);
		} catch (NumberFormatException e) {
			e.printStackTrace();
			System.out.println("Fehler"+line);
		}catch(IllegalArgumentException e){
			LOG.debug("Tupel does not match required LRB format" + line);
			System.out.println("Fehler"+line);
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void close() {
		// in.close();
		try {
			cnt.stop();
			LOG.debug("Simulation Duration " + cnt.getDurationTimeSecs()+ " mit "+tupleCnt);
			isr.close();
			in.close();
			clientSocket.close();
		} catch (IOException e) {
			e.printStackTrace();
			LOG.debug("IOException..",e);
		}

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Config conf = new Config();
		conf.setMaxTaskParallelism(1);
		return conf;
	}

	@Override
	public void ack(Object id) {
	}

	@Override
	public void fail(Object id) {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("stream", new Fields("tuple","StormTimer"));
		
	}

}