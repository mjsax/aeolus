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
import storm.lrb.model.AccBalRequest;
import storm.lrb.model.DaiExpRequest;
import storm.lrb.model.PosReport;
import storm.lrb.model.TTEstRequest;

/**
 * 
 * Spout that connects to the given host and socket to process the input stream.
 * It dispatches the input tuples according to their type and additional splits 
 * all position reports to an individual stream for each expressway.
 * 
 */
@SuppressWarnings("serial")
public class SocketClientSplitSpout extends BaseRichSpout {
	SpoutOutputCollector _collector;
	LinkedBlockingQueue<String> queue = null;
	String _host;
	int _port;
	Socket clientSocket;
	InputStreamReader isr;
	BufferedReader in;
	StopWatch cnt;
	int _processed_xway;
	boolean firstrun=true;

	private static final Logger LOG = Logger.getLogger(SocketClientSplitSpout.class);

	public SocketClientSplitSpout(String host, int port,  int xways) {
		_host = host;
		_port = port;
		cnt = new StopWatch();
		_processed_xway = xways; 
		
	}

	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf,
			TopologyContext context, SpoutOutputCollector collector) {
		queue = new LinkedBlockingQueue<String>(20);
		_collector = collector;

			boolean goon = true;
		
		//200 seconds to establish connection
		while (cnt.getElapsedTimeSecs()  <= 200) {
			try {
				if (cnt.getElapsedTimeSecs() > 2)	Utils.sleep(5000);

				if (goon == true) 	goon = false;
				else continue;
				
				clientSocket = new Socket(InetAddress.getByName(_host), _port);
				LOG.info("Connection to " + _host + ":" + _port
						+ " established.\t StormTimer: "+ cnt.getElapsedTimeSecs()+"s");
				System.out.println("######connection established");
				cnt.start(cnt.getOffset());
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

		//TODO evtl. vorher checken ob  in überhaupt da
		try {
			if ((line = in.readLine()) != null
					&& line.substring(0, 1).matches("[0-4]")) {
				splitAndEmit(line);
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
	}

	private void splitAndEmit(String line) {

		String tmp = line.substring(0,1);
		
		//System.out.println("check"+ tmp);
		try {
			if(firstrun){
				firstrun = false;
				int offset = Integer.parseInt(line.substring(2, line.indexOf(",", 2)));
				LOG.info("Simulation starts with offset "+offset);
				cnt.start(offset);;
			}
			switch (Integer.parseInt(tmp)) {
			case 0:
				PosReport pos = new PosReport(line, cnt);
				
				String output_stream = "PosReports_"+pos.getXway().toString();
				_collector.emit(output_stream,
						new Values(pos.getXway(), pos.getXsd(),pos.getVid(),  pos));
				break;
			case 2:
				AccBalRequest acc = new AccBalRequest(line, cnt);
				_collector.emit("AccBalRequests", new Values(acc.getVid(),acc));
				break;
			case 3:
				DaiExpRequest exp = new DaiExpRequest(line, cnt);
				_collector.emit("DaiExpRequests",new Values(exp.getVid(),  exp));
				break;
			case 4:
				TTEstRequest est = new TTEstRequest(line, cnt);
				_collector.emit("TTEstRequests", new Values(est.getVid(),est));
				break;
			default:
				System.out.println("Unknown tuple");
				LOG.debug("Tupel does not match required LRB format" + line);
				
			}
		}catch (NumberFormatException e) {
			e.printStackTrace();
			System.out.println("Fehler"+line);
		}catch(IllegalArgumentException e){
			System.out.println("Fehler"+line);
			e.printStackTrace();
		}

	}

	@Override
	public void close() {
		// in.close();
		try {
			cnt.stop();
			System.out.println("Simulation Duration " + cnt.getDurationTimeSecs());
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
		declarer.declareStream("StormTimer", new Fields("StormTimer"));
		for ( int faktor = 0; faktor < _processed_xway ; faktor ++ ) {
			declarer.declareStream("PosReports_"+faktor, new Fields("xway", "xsd", "vid", "PosReport"));
		}
		
		//declarer.declareStream("PosReports", new Fields("xway", "xsd", "vid", "PosReport"));
		
		declarer.declareStream("AccBalRequests", new Fields("vid", "AccBalRequests"));
		declarer.declareStream("DaiExpRequests", new Fields("vid","DaiExpRequests"));
		declarer.declareStream("TTEstRequests", new Fields("vid","TTEstRequests"));
	}

}