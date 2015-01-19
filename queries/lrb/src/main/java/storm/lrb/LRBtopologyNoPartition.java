package storm.lrb;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

//import backtype.storm.contrib.cassandra.bolt.CassandraBatchingBolt;
//import backtype.storm.contrib.cassandra.bolt.CassandraBolt;
//import backtype.storm.contrib.cassandra.bolt.CassandraCounterBatchingBolt;



import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Locale;

import org.joda.time.DateTime;

import com.beust.jcommander.JCommander;

import storm.lrb.spout.SocketClientSpout;
import storm.lrb.tools.CommandLineParser;
import storm.lrb.tools.StopWatch;
import storm.lrb.bolt.AccidentDetectionBolt;
import storm.lrb.bolt.AccidentNotificationBolt;
import storm.lrb.bolt.AccountBalanceBolt;
import storm.lrb.bolt.AvgsBolt;
import storm.lrb.bolt.DailyExpenditureBolt;
import storm.lrb.bolt.FileWriterBolt;
import storm.lrb.bolt.LavBolt;
import storm.lrb.bolt.SegmentStatsBolt;
import storm.lrb.bolt.TollNotificationBolt;

/**
 * This LRBtopology serves as reference implementation. Parallelism hint of is
 * set to 1. The only configurable parameters accepted are the number of tasks the
 * tollnotificationbolt should be initialized with and how many workers should be used.
 */
public class LRBtopologyNoPartition {

	public static void main(String[] args) throws Exception {
	
		CommandLineParser cmd = new CommandLineParser();

		new JCommander(cmd, args);

		System.out.println("host: " + cmd.host + " port: " + cmd.port);
		System.out.println("Submit to cluster: " + cmd.submit);
		if(cmd.offset==0) System.out.println("using offset: " + cmd.offset);
		StopWatch stormTimer = new StopWatch(cmd.offset);


		TopologyBuilder builder = new TopologyBuilder();

		String sockethost = cmd.host;
		int socketport = cmd.port;
		int offset = cmd.offset;
		String _lr = cmd.nameext + "_lrbnopartition_" + "_L" + cmd.xways + "_"
				+ cmd.workers + "W_T" + cmd.tasks + "_1E";
	

		builder.setSpout("Spout", new SocketClientSpout(sockethost, socketport), 1);
		stormTimer.start(offset);

//		builder.setBolt("lavBolt", new SegmentStatsBolt(0), 1)
//				.fieldsGrouping("Spout", "PosReports", new Fields("xsd"));
		builder.setBolt("avgsBolt", new AvgsBolt(cmd.xways), cmd.xways*3)
				.fieldsGrouping("SplitStreamBolt", "PosReports", new Fields("xsd"));

		builder.setBolt("lavBolt", new LavBolt(cmd.xways), cmd.xways*3)
				.fieldsGrouping("avgsBolt",  new Fields("xsd"));

		builder.setBolt("tollnotification",new TollNotificationBolt(stormTimer, 0), 1)
				.setNumTasks(cmd.tasks)
				.allGrouping("lavBolt")
				.allGrouping("accidentdetection")
				.allGrouping("Spout", "PosReports");
		

		builder.setBolt("tollfilewriter",new FileWriterBolt(_lr + "_toll", 16, cmd.submit), 1)
				.allGrouping("tollnotification", "TollNotification_0");

		builder.setBolt("accidentdetection", new AccidentDetectionBolt(0), 1)
				.allGrouping("Spout", "PosReports");

		builder.setBolt("accnotification", new AccidentNotificationBolt(), 1)
				.allGrouping("accidentdetection")
				.allGrouping("Spout", "PosReports");

		builder.setBolt("accfilewriter", new FileWriterBolt(_lr + "_acc", 8, cmd.submit), 1)
				.allGrouping("accnotification");

		builder.setBolt("accountbalance", new AccountBalanceBolt(), 1)
				.allGrouping("Spout","AccBalRequests")
				.allGrouping("tollnotification", "TollAssessment_0");

		builder.setBolt("balfilewriter", new FileWriterBolt(_lr + "_bal", 8, cmd.submit), 1)
				.allGrouping("accountbalance");

		
		builder.setBolt("dailyexpenditure", new
		DailyExpenditureBolt(cmd.histFile), 1) .shuffleGrouping("Spout","DaiExpRequests");
		 
		builder.setBolt("expfilewriter", new FileWriterBolt(_lr+"_exp", 4, cmd.submit), 1) .allGrouping("dailyexpenditure");
		 
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
		if (cmd.submit)
			System.setOut(new PrintStream(new OutputStream() {
				public void write(int b) {
					// NO-OP
				}
			}));

		Locale.setDefault(new Locale("en", "US"));

		System.out.println("Starte: " + _lr);

		if (args != null && cmd.submit) {

			conf.setNumWorkers(cmd.workers);

			StormSubmitter.submitTopology(_lr, conf, builder.createTopology());

		} else {

			LocalCluster cluster = new LocalCluster();

			cluster.submitTopology("test_lrb", conf, builder.createTopology());

			Utils.sleep(100000);
			cluster.killTopology("test_lrb");
			cluster.shutdown();
		}
	}
}
