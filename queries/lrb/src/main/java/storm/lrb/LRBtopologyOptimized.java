package storm.lrb;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Locale;

import org.joda.time.DateTime;

import com.beust.jcommander.JCommander;

import storm.lrb.spout.DataDriverSpout;
import storm.lrb.spout.SocketClientSpoutPure;
import storm.lrb.tools.CommandLineParser;
import storm.lrb.tools.Helper;
import storm.lrb.tools.StopWatch;
import storm.lrb.bolt.AccidentDetectionBolt;
import storm.lrb.bolt.AccidentNotificationBolt;
import storm.lrb.bolt.AccountBalanceBolt;
import storm.lrb.bolt.AvgsBolt;
import storm.lrb.bolt.DailyExpenditureBolt;
import storm.lrb.bolt.DispatcherSplitBolt;
import storm.lrb.bolt.FileWriterBolt;
import storm.lrb.bolt.LavBolt;
import storm.lrb.bolt.SegmentStatsBolt;
import storm.lrb.bolt.TollNotificationBolt;

//import storm.lrb.spout.SocketClientSpout;

/**
 * This LRBtopology corresponds to Lrbtopology but uses another fieldgrouping with the tollnotification bolt
 * 
 */
public class LRBtopologyOptimized {

	public static void main(String[] args) throws Exception {
		
		CommandLineParser cmd = new CommandLineParser();

		new JCommander(cmd, args);

		System.out.println("host: " + cmd.host + " port: " + cmd.port);
		System.out.println("Submit to cluster: " + cmd.submit);
		if(cmd.offset!=0) System.out.println("using offset: " + cmd.offset);
		StopWatch stormTimer = new StopWatch(cmd.offset);

		TopologyBuilder builder = new TopologyBuilder();

		int executors = cmd.executors;
		
		
		int tasks = executors*cmd.tasks;
		String _lr = cmd.nameext+"_lrbNormal_" + Helper.readable(cmd.fields)+"_L"+cmd.xways+"_"
					+cmd.workers+"W_T"+tasks+"_"+executors+"E_O"+cmd.offset;
		
		
		
		builder.setSpout("Spout", new SocketClientSpoutPure(cmd.host,cmd.port), 1);
			//builder.setSpout("Spout", new DataDriverSpout(cmd.file), 1);
		
		stormTimer.start(cmd.offset);
		
		
		builder.setBolt("SplitStreamBolt",new DispatcherSplitBolt(cmd.xways), cmd.xways)
				.setNumTasks(tasks).shuffleGrouping("Spout", "stream");

		//dann nochmal mit avg testen!!
//		for (int faktor = 0; faktor < cmd.xways; faktor++) {
//			String out = "lavBolt_" + faktor;
//			String in = "PosReports_" + faktor;
//			builder.setBolt(out, new SegmentStatsBolt(faktor), 3)
//				.fieldsGrouping("SplitStreamBolt", in, new Fields("xsd"));
//
//		}

		for (int faktor = 0; faktor < cmd.xways; faktor++) {
			String out = "avgsBolt_" + faktor;
			String in = "PosReports_" + faktor;
			builder.setBolt(out, new AvgsBolt(cmd.xways), cmd.xways*3)
				.fieldsGrouping("SplitStreamBolt", in, new Fields("xsd"));
		}

		
	
		for (int faktor = 0; faktor < cmd.xways; faktor++) {
			String out = "lavBolt_" + faktor;
			String in = "avgsBolt_" + faktor;
			builder.setBolt(out, new LavBolt(cmd.xways), cmd.xways*3)
				.fieldsGrouping(in, new Fields("xsd"));
		}
		
		for (int faktor = 0; faktor < cmd.xways; faktor++) {
			String out = "tollnotification_" + faktor;
			String in = "PosReports_" + faktor;
			String in2 = "lavBolt_" + faktor;
			String in3 = "accidentdetection_" + faktor;
			builder.setBolt(out, new TollNotificationBolt(stormTimer, faktor),cmd.executors)
				.setNumTasks(tasks).fieldsGrouping(in2, new Fields(cmd.fields))
				.fieldsGrouping(in3, new Fields(cmd.fields))
				.fieldsGrouping("SplitStreamBolt", in, new Fields(cmd.fields));

		}
		

		BoltDeclarer buildtemp = builder.setBolt("tollfilewriter",
									new FileWriterBolt(_lr+"_toll", cmd.xways*8, cmd.submit), 1);

		for (int i = 0; i < cmd.xways; i++) {
			String inbolt = "tollnotification_" + Integer.toString(i);
			String instream = "TollNotification_" + Integer.toString(i);
			buildtemp = buildtemp.allGrouping(inbolt, instream);
		}
	
		for (int faktor = 0; faktor < cmd.xways; faktor++) {
			String out = "accidentdetection_" + faktor;
			String in = "PosReports_" + faktor;
			builder.setBolt(out, new AccidentDetectionBolt(faktor), 2)
				.fieldsGrouping("SplitStreamBolt", in, new Fields("xd"));

		}

		//alternativ hier mit fields statt allgrouping auf xd
		for (int faktor = 0; faktor < cmd.xways; faktor++) {
			String out = "accnotification_" + faktor;
			String in = "PosReports_" + faktor;
			String in2 = "accidentdetection_" + faktor;
			builder.setBolt(out, new AccidentNotificationBolt(), 2)
				.fieldsGrouping(in2, new Fields("xd"))
				.fieldsGrouping("SplitStreamBolt", in, new Fields("xd"));

		}
		buildtemp = builder.setBolt("accfilewriter", new FileWriterBolt(_lr+"acc_", cmd.xways*2, cmd.submit), 1);

		for (int faktor = 0; faktor < cmd.xways; faktor++) {
			String inbolt = "accnotification_" + faktor;
			buildtemp = buildtemp.allGrouping(inbolt);
		}
		
		buildtemp = builder.setBolt("accountbalance", new AccountBalanceBolt(), cmd.xways * 2)
				.fieldsGrouping("SplitStreamBolt", "AccBalRequests", new Fields("vid"));

		for (int faktor = 0; faktor < cmd.xways; faktor++) {
			String inbolt = "tollnotification_" + faktor;
			String instream = "TollAssessment_" + faktor;
			buildtemp = buildtemp.fieldsGrouping(inbolt, instream, new Fields("vid"));
		}

		builder.setBolt("balfilewriter",new FileWriterBolt(_lr+"_bal", cmd.xways*2, cmd.submit), 1)
				.allGrouping("accountbalance");

		builder.setBolt("dailyexpenditure", new DailyExpenditureBolt(cmd.histFile),cmd.xways * 1)
				.shuffleGrouping("SplitStreamBolt", "DaiExpRequests");
		builder.setBolt("expfilewriter",new FileWriterBolt(_lr+"_exp", cmd.xways*1, cmd.submit), 1)
				.allGrouping("dailyexpenditure");

	
		
		Config conf = new Config();
		conf.setDebug(cmd.debug);

		conf.registerSerialization(storm.lrb.model.PosReport.class);
		conf.registerSerialization(storm.lrb.model.AccBalRequest.class);
		conf.registerSerialization(storm.lrb.model.DaiExpRequest.class);
		conf.registerSerialization(storm.lrb.model.TTEstRequest.class);
		conf.registerSerialization(storm.lrb.model.Accident.class);
		conf.registerSerialization(storm.lrb.model.VehicleInfo.class);
		conf.registerSerialization(storm.lrb.model.LRBtuple.class);
		conf.registerSerialization(storm.lrb.tools.StopWatch.class);
		conf.registerSerialization(storm.lrb.model.AccidentImmutable.class);

		/* cluster testing */
		if(cmd.submit)
		System.setOut(new PrintStream(new OutputStream() {
			  public void write(int b) {
			    // NO-OP
			  }
			}));	
		
		Locale.setDefault(new Locale("en", "US"));

		System.out.println("Starte: " + _lr);
		
		if (args != null && cmd.submit) {
			
			int workers = cmd.workers;
			conf.setNumWorkers(workers);
			conf.setNumAckers(workers);

			StormSubmitter.submitTopology(_lr, conf, builder.createTopology());

		} else {

			LocalCluster cluster = new LocalCluster();

			cluster.submitTopology("test_lrb", conf, builder.createTopology());

			Utils.sleep(2546000);
			cluster.killTopology("test_lrb");
			cluster.shutdown();
		}
	}
}
