package storm.lrb.spout;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.lrb.TopologyControl;
import storm.lrb.bolt.DispatcherBolt;
import storm.lrb.tools.StopWatch;
import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;





/**
 * This Spout connects to the given host and socket, reads tuples line by line and emits it to the default stream. This
 * spout is used to make reading from socket faster by defering LRBtuple creation to the {@link DispatcherBolt} which
 * can be parallelized
 * 
 */
public class SocketClientSpoutPure extends BaseRichSpout {
	
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(SocketClientSpoutPure.class);
	private SpoutOutputCollector collector;
	private final String host;
	private final int port;
	private Socket clientSocket;
	private InputStreamReader isr;
	private BufferedReader in;
	private final StopWatch cnt;
	private long tupleCnt = 0;
	private boolean firstrun = true;
	
	public SocketClientSpoutPure(String host, int port) {
		this.host = host;
		this.port = port;
		this.cnt = new StopWatch();
	}
	
	@Override
	@SuppressWarnings("SleepWhileInLoop")
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		
		boolean goon = true;
		// try no more than 2 minutes to connect
		while(this.cnt.getElapsedTimeSecs() <= 120) {
			try {
				
				if(this.cnt.getElapsedTimeSecs() > 2) {
					Utils.sleep(5000);
				}
				
				if(goon == true) {
					goon = false;
				} else {
					break;
				}
				
				this.clientSocket = new Socket(InetAddress.getByName(this.host), this.port);
				LOG.info("Connection to " + this.host + ":" + this.port + " established.\t StormTimer: "
					+ this.cnt.getElapsedTimeSecs() + "s");
				
				this.isr = new InputStreamReader(this.clientSocket.getInputStream());
				this.in = new BufferedReader(this.isr);
				
			} catch(java.net.ConnectException e) {
				goon = true;
				LOG.warn(String.format(
					"failed to connect to host '%s' on port %d (see following exception for details), retrying...",
					this.host, this.port), e);
				Utils.sleep(2000);
			} catch(UnknownHostException e) {
				throw new RuntimeException(e);
			} catch(IOException e) {
				throw new RuntimeException(e);
			}
		}
		
	}
	
	@Override
	public void nextTuple() {
		String line = "";
		try {
			line = this.in.readLine();
			if(line != null) {
				if(line.startsWith("#")) {
					return;
				}
				if(this.firstrun) {
					int offset = Integer.parseInt(line.substring(2, line.indexOf(',', 2)));
					LOG.info("Simulation starts with offset " + offset);
					this.cnt.start(offset);
					this.firstrun = false;
				}
				// LOG.info(line+ " at "+cnt.getElapsedTimeSecs());
				this.collector.emit(TopologyControl.SPOUT_STREAM_ID, new Values(line, this.cnt), this.tupleCnt);
				this.tupleCnt++;
				
			} else {
				int waitMillis = 50;
				LOG.debug("Waiting %d millis for the next tuple to arrive", waitMillis);
				Utils.sleep(waitMillis);
			}
		} catch(NumberFormatException e) {
			LOG.error("Error in line '%s'", line);
			throw new RuntimeException(e);
		} catch(IllegalArgumentException e) {
			LOG.error("Error in line '%s'", line);
			throw new RuntimeException(e);
		} catch(IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public void close() {
		// in.close();
		try {
			this.cnt.stop();
			LOG.debug("Simulation duration: %d s; # of tuples: ", this.cnt.getDurationTimeSecs(), this.tupleCnt);
			this.isr.close();
			this.in.close();
			this.clientSocket.close();
		} catch(IOException e) {
			throw new RuntimeException(e);
		}
		
	}
	
	@Override
	public Map<String, Object> getComponentConfiguration() {
		Config conf = new Config();
		conf.setMaxTaskParallelism(1);
		return conf;
	}
	
	@Override
	public void ack(Object id) {}
	
	@Override
	public void fail(Object id) {}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(TopologyControl.SPOUT_STREAM_ID, new Fields(TopologyControl.TUPLE_FIELD_NAME,
			TopologyControl.TIMER_FIELD_NAME));
	}
	
}
