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
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
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
 * UDP version of the socketclientspout.
 */
@SuppressWarnings("serial")
public class UDPSocketClientSplitSpout extends BaseRichSpout {
	SpoutOutputCollector _collector;
	LinkedBlockingQueue<String> queue = null;
	String _host;
	int _port;
	Socket clientSocket;
	public static DatagramSocket datagramClient;
	public static DatagramPacket receiverPacket;
	InputStreamReader isr;
	BufferedReader in;
	StopWatch cnt;

	private static final Logger LOG = Logger
			.getLogger(UDPSocketClientSplitSpout.class);

	public UDPSocketClientSplitSpout(String host, int port) {
		_host = host;
		_port = port;
	}

	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf,
			TopologyContext context, SpoutOutputCollector collector) {
		queue = new LinkedBlockingQueue<String>(20);
		_collector = collector;
		
		cnt = new StopWatch();
		//cnt = new StopWatch(2896);
		boolean goon = true;

	
			try {
				datagramClient	= new DatagramSocket();
				LOG.info("Opened DatagramSocket on" + _host + ":" + _port
						+ ".");
				System.out.println("######connection established:"+datagramClient.isConnected());
				cnt.start();
				//TODO timer starten wenn ests tupel geholt wurde!!!
				
			} catch (java.net.ConnectException e) {
				goon = true;
				LOG.info("Trying to connect...");
				System.out.println("Trying to connect...");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				LOG.error("IOexception...",e);
			}
		

	}

	@Override
	public void nextTuple() {

		String line = "";

		//TODO evtl. vorher checken ob  in überhaupt da timeout inbauen falls keine antwort?!
		try {
			
			
			
            byte sendData[]			= new byte[1024];
           
            sendData = "GET".getBytes();
            DatagramPacket packet 	= new DatagramPacket(sendData, 0,sendData.length, InetAddress.getByName(_host), _port);
            datagramClient.send(packet);
            byte[] recv = new byte[100];
            receiverPacket	= new DatagramPacket(recv, recv.length);
            datagramClient.receive(receiverPacket);
            line  = (new String(recv)).trim();
            System.out.println("received: "+line);
            if(line != null && line.substring(0, 4).matches("Wait")){
            	String[] tmp = line.split(":");
            	long msecs = Long.parseLong(tmp[1]);
            	Utils.sleep(msecs);
            	System.out.println("Hold for "+msecs);
            }else if (line != null
					&& line.substring(0, 1).matches("[0-4]")) {
    			
				splitAndEmit(line.trim());
			} else if (line != null && line.startsWith("###"))
				System.out.println(line);
            else 
				Utils.sleep(50);
			
			 
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			LOG.debug("Tupel does not match required LRB format" + line);
		} /*catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
	}

	private void splitAndEmit(String line) {

		String tmp = line.substring(0,1);
		//System.out.println("check"+ tmp);
		try {
			switch (Integer.parseInt(tmp)) {
			case 0:
				PosReport pos = new PosReport(line, cnt);
				System.out.println("emit: "+pos);
				_collector.emit("PosReports",
						new Values(pos.getXsd(),pos.getVid(),  pos));
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
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("ääähm");
		}

		// _collector.emit(new Values(line));
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
		declarer.declareStream("PosReports", new Fields("xsd", "vid", "PosReport"));
		declarer.declareStream("AccBalRequests", new Fields("vid", "AccBalRequests"));
		declarer.declareStream("DaiExpRequests", new Fields("vid","DaiExpRequests"));
		declarer.declareStream("TTEstRequests", new Fields("vid","TTEstRequests"));
	}

}