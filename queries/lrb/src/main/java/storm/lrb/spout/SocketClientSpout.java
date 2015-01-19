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
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import storm.lrb.tools.StopWatch;
import storm.lrb.bolt.LavBolt;
import storm.lrb.model.AccBalRequest;
import storm.lrb.model.DaiExpRequest;
import storm.lrb.model.LRBtuple;
import storm.lrb.model.PosReport;
import storm.lrb.model.TTEstRequest;


/**
 * This Spout connects to the given host and socket, reads tuples line by line
 * and generates LRBtuples and emits it to the appropiate stream according to 
 * the tuple type.
 * 
 */
@SuppressWarnings("serial")
public class SocketClientSpout extends BaseRichSpout {
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
	StopWatch timer;

	private static final Logger LOG = Logger
			.getLogger(SocketClientSpout.class);

	/**
	 * SocketClientSpout constructor
	 * @param host
	 * @param port
	 * @param stormTimer
	 */
	public SocketClientSpout(String host, int port) {
		_host = host;
		_port = port;
		cnt = new StopWatch(0);
		//_processed_xway = xways; 
		
	}

	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf,
			TopologyContext context, SpoutOutputCollector collector) {
		//queue = new LinkedBlockingQueue<String>(20);
		_collector = collector;

		
		boolean goon = true;
		
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
				// TODO Auto-generated catch block
				e.printStackTrace();
				LOG.error("IOexception...",e);
			}
		}

	}

	@Override
	public void nextTuple() {
		String line = "";
		LRBtuple t=null;
		try {
			if ((line = in.readLine()) != null && line.substring(0, 1).matches("[0-4]")) {
				if(firstrun){
					firstrun = false;
					int offset = Integer.parseInt(line.substring(2, line.indexOf(",", 2)));
					LOG.info("Simulation starts with offset "+offset);
					cnt.start(offset);;	
				};
				splitAndEmit(line);
				LOG.info("Processed "+line +" at "+cnt.getElapsedTimeSecs());
			} else if (line != null && line.startsWith("###"))
				System.out.println(line);
			else if (line == null)
				Utils.sleep(50);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			LOG.debug("Tupel does not match required LRB format" + line);
		}
		//timer.stop();
		//LOG.debug("SPOUT: " +timer.getElapsedTime()+"ms needed to create " +t);
	}

	private void splitAndEmit(String line) {

		String tmp = line.substring(0,1);
		LRBtuple t = null;
		try {
			if(firstrun){
				firstrun = false;
				int offset = Integer.parseInt(line.substring(2, line.indexOf(",", 2)));
				LOG.info("Simulation starts with offset "+ offset);
				cnt.start(offset);;
				LOG.info("SocketClientSpout resets timer:"+cnt);
			}
			switch (Integer.parseInt(tmp)) {
			case 0:
				PosReport pos = new PosReport(line, cnt);
				t = pos;
				String output_stream = "PosReports";
				//System.out.println("SOCKET: emit: "+pos +" nach "+ output_stream);
				//if(cnt.getElapsedTimeSecs()%300==0 || cnt.getElapsedTimeSecs()>10780){
					//LOG.info("SOCKET:: emit: "+pos +" nach "+ output_stream+ " aus "+line );
				//}
				
				_collector.emit(output_stream,
						new Values(pos.getXway(),pos.getDir(), pos.getXD(), pos.getXsd(),pos.getVid(),  pos), tupleCnt);
				break;
			case 2:
				AccBalRequest acc = new AccBalRequest(line, cnt);
				t = acc;
				_collector.emit("AccBalRequests", new Values(acc.getVid(),acc) , tupleCnt);
				break;
			case 3:
				DaiExpRequest exp = new DaiExpRequest(line, cnt);
				t= exp;
				_collector.emit("DaiExpRequests",new Values(exp.getVid(),  exp), tupleCnt);
				break;
			case 4:
				TTEstRequest est = new TTEstRequest(line, cnt);
				t=est;
				_collector.emit("TTEstRequests", new Values(est.getVid(),est), tupleCnt);
				break;
			default:
				t=null;
				LOG.debug("Tupel does not match required LRB format" + line);
				
			}
			tupleCnt++;
		}catch (NumberFormatException e) {
			e.printStackTrace();
			System.out.println("Fehler"+line);
		}catch(IllegalArgumentException e){
			System.out.println("Fehler"+line);
			e.printStackTrace();
		}
		
		// _collector.emit(new Values(line));
	}

	@Override
	public void close() {
		// in.close();
		try {
			cnt.stop();
			LOG.debug("Simulation Duration " + cnt.getDurationTimeSecs()+ " mit "+tupleCnt);
			System.out.println("Simulation Duration " + cnt.getDurationTimeSecs()+ " mit "+tupleCnt);
			isr.close();
			in.close();
			clientSocket.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			LOG.debug("IOException..",e);
		}

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Config conf = new Config();
		conf.setMaxTaskParallelism(1);
		// conf.registerSerialization(PosReport.class);???
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
		declarer.declareStream("PosReports", new Fields("xway","dir","xd", "xsd", "vid", "PosReport"));
		
		
		//declarer.declareStream("PosReports", new Fields("xway", "xsd", "vid", "PosReport"));
		
		declarer.declareStream("AccBalRequests", new Fields("vid", "AccBalRequests"));
		declarer.declareStream("DaiExpRequests", new Fields("vid","DaiExpRequests"));
		declarer.declareStream("TTEstRequests", new Fields("vid","TTEstRequests"));
	}

}