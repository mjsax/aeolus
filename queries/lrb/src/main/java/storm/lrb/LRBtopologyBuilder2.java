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
package storm.lrb;

import backtype.storm.topology.TopologyBuilder;





//import storm.lrb.spout.SocketClientSpout;
/**
 * Thistopology is equivalent to LrbXD with the difference, that "xway" and "dir" are used in fieldsgrouping of the
 * tollnotificationbolt as opposed to LrbXD where one field is made out of both fields two.
 */
public class LRBtopologyBuilder2 extends TopologyBuilder {
	
	public LRBtopologyBuilder2() {
		
		// CommandLineParser cmd = new CommandLineParser();
		//
		// System.out.println("host: " + cmd.host + " port: " + cmd.port);
		// System.out.println("Submit to cluster: " + cmd.submit);
		// if (cmd.offset != 0) {
		// System.out.println("using offset: " + cmd.offset);
		// }
		// StopWatch stormTimer = new StopWatch(cmd.offset);
		//
		// TopologyBuilder builder = new TopologyBuilder();
		//
		// int executors = cmd.executors;
		// if (cmd.xways > 1) {
		// executors = (cmd.xways / 2) * cmd.executors;
		// }
		// int tasks = executors * cmd.tasks;
		// String _lr = cmd.nameext + "_lrbNormal_" + Helper.readable(cmd.fields) + "_L" + cmd.xways + "_"
		// + cmd.workers + "W_T" + tasks + "_" + executors + "E_O" + cmd.offset;
		//
		// int xways = cmd.xways;
		// String histfile = cmd.histFile;
		//
		// builder.setSpout("Spout", new SocketClientSpoutPure(cmd.host, cmd.port), 1);
		//
		// builder.setBolt("SplitStreamBolt", new DispatcherBolt(), 2 * cmd.xways).setNumTasks(4 * cmd.xways)
		// .shuffleGrouping("Spout", "stream");//.allGrouping("Spout", "stormtimer");
		//
		// builder.setBolt("avgsBolt", new AvgsBolt(cmd.xways), cmd.xways * 3)
		// .fieldsGrouping("SplitStreamBolt", "PosReports", new Fields("xsd"));
		//
		// builder.setBolt("lavBolt", new LavBolt(cmd.xways), cmd.xways * 3)
		// .fieldsGrouping("avgsBolt", new Fields("xsd"));
		//
		// //builder.setBolt("lavBolt", new SegmentStatsBolt(0), cmd.xways*3)
		// //.fieldsGrouping("SplitStreamBolt", "PosReports", new Fields("xsd"));
		// builder.setBolt("tollnotification", new TollNotificationBolt(stormTimer, 0), executors)
		// .setNumTasks(tasks).fieldsGrouping("lavBolt", new Fields(cmd.fields))
		// .fieldsGrouping("accidentdetection", new Fields(cmd.fields))
		// .fieldsGrouping("SplitStreamBolt", "PosReports", new Fields(cmd.fields));
		//
		// builder.setBolt("tollfilewriter", new FileWriterBolt(_lr + "_toll", cmd.xways * 8, cmd.submit), 1)
		// .allGrouping("tollnotification", "TollNotification_0");
		//
		// builder.setBolt("accidentdetection", new AccidentDetectionBolt(0), cmd.xways)
		// .fieldsGrouping("SplitStreamBolt", "PosReports", new Fields("xd"));
		//
		// builder.setBolt("accnotification", new AccidentNotificationBolt(), cmd.xways)
		// .fieldsGrouping("accidentdetection", new Fields("xd"))
		// .fieldsGrouping("SplitStreamBolt", "PosReports", new Fields("xd"));
		//
		// builder.setBolt("accfilewriter", new FileWriterBolt(_lr + "_acc", cmd.xways * 4, cmd.submit), 1)
		// .allGrouping("accnotification");
		//
		// builder.setBolt("accountbalance", new AccountBalanceBolt(), cmd.xways)
		// .fieldsGrouping("SplitStreamBolt", "AccBalRequests", new Fields("vid"))
		// .fieldsGrouping("tollnotification", "TollAssessment_0", new Fields("vid"));
		//
		// builder.setBolt("balfilewriter", new FileWriterBolt(_lr + "_bal", cmd.xways * 4, cmd.submit), 1)
		// .allGrouping("accountbalance");
		//
		// builder.setBolt("dailyexpenditure", new DailyExpenditureBolt(histfile), xways * 1)
		// .shuffleGrouping("SplitStreamBolt", "DaiExpRequests");
		//
		// builder.setBolt("expfilewriter", new FileWriterBolt(_lr + "_exp", cmd.xways * 2, cmd.submit), 1)
		// .allGrouping("dailyexpenditure");
		//
		// Config conf = new Config();
		// conf.setDebug(cmd.debug);
		//
		// conf.registerSerialization(storm.lrb.model.PosReport.class);
		// conf.registerSerialization(storm.lrb.model.AccBalRequest.class);
		// conf.registerSerialization(storm.lrb.model.DaiExpRequest.class);
		// conf.registerSerialization(storm.lrb.model.TTEstRequest.class);
		// conf.registerSerialization(storm.lrb.model.Accident.class);
		// conf.registerSerialization(storm.lrb.model.VehicleInfo.class);
		// conf.registerSerialization(storm.lrb.model.LRBtuple.class);
		// conf.registerSerialization(storm.lrb.tools.StopWatch.class);
		// conf.registerSerialization(storm.lrb.model.AccidentImmutable.class);
		//
		// /* cluster testing */
		// /*
		// * String path = (String) conf.get(Config.STORM_LOCAL_DIR);
		// * System.out.println(path); System.setOut(new PrintStream(new
		// * FileOutputStream("/var/storm/output.txt")));
		// */
		//
		// /* cluster testing */
		// if (cmd.submit) {
		// System.setOut(new PrintStream(new OutputStream() {
		// @Override
		// public void write(int b) {
		// // NO-OP
		// }
		// }));
		// }
		//
		// Locale.setDefault(new Locale("en", "US"));
		//
		// System.out.println("Starte: " + "stormlrb" + _lr);
		//
		// if (args != null && cmd.submit) {
		//
		// int workers = cmd.workers;
		// conf.setNumWorkers(workers);
		// conf.setNumAckers(workers);
		//
		// StormSubmitter.submitTopology(_lr, conf, builder.createTopology());
		//
		// } else {
		//
		// LocalCluster cluster = new LocalCluster();
		//
		// cluster.submitTopology("test_lrb", conf, builder.createTopology());
		//
		// Utils.sleep(2100000);
		// cluster.killTopology("test_lrb");
		// cluster.shutdown();
		// }
	}
}
