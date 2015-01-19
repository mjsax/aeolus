package storm.lrb.bolt;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;


import storm.lrb.model.AccBalRequest;
import storm.lrb.model.PosReport;
import storm.lrb.model.VehicleAccount;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * 
 * This bolt recieves the toll values assesed by the tollnotificationbolt and
 * answers to account balance queries.
 * 
 */
public class AccountBalanceBolt extends BaseRichBolt {
	
	private static final long serialVersionUID = 1L;

	OutputCollector _collector;

	private static final Logger LOG = Logger.getLogger(AccountBalanceBolt.class);
	
	/**
	 * Contains all vehicles and the accountinformation of the current day.
	 */
	private ConcurrentHashMap<Integer, VehicleAccount> allVehicles;
	
	
	@Override
	public void prepare(Map conf, TopologyContext context,
			OutputCollector collector) {
		_collector = collector;
		allVehicles = new ConcurrentHashMap<Integer, VehicleAccount>();
	}

	@Override
	public void execute(Tuple tuple) {

		Fields fields = tuple.getFields();

		if (fields.contains("AccBalRequests")) {
			getBalanceAndSend(tuple);
		}

		if (tuple.contains("tollAssessed")) {
		
			updateBalance(tuple);
		} 
		
		_collector.ack(tuple);
	}

	private void updateBalance(Tuple tuple) {
		Integer vid = tuple.getIntegerByField("vid");
		VehicleAccount account = allVehicles.get(vid);
		PosReport pos = (PosReport) tuple.getValueByField("PosReport");
		if(account==null){	
			account = new VehicleAccount(tuple.getIntegerByField("tollAssessed"), pos);
			allVehicles.put(tuple.getIntegerByField("vid"), account);
		}else
			account.assessToll(tuple.getIntegerByField("tollAssessed"), pos.getEmitTime());
	}

	private void getBalanceAndSend(Tuple tuple) {
		AccBalRequest bal = (AccBalRequest) tuple.getValueByField("AccBalRequests");
		VehicleAccount account = allVehicles.get(bal.getVehicleIdentifier());
	
		if(account==null){
			LOG.debug("No account information available yet: at:"+bal.getTime()+" for request" + bal);
			String notification = "2," + bal.getTime()+","+bal.getEmitTime()+","+ bal.getQueryIdentifier() + ","+ 0 + ","+  0 + "###"+bal.toString()+"###";
			//_collector.emit(tuple, new Values(notification));
			_collector.emit(new Values(notification));
		}
		else
			_collector.emit(new Values(account.getAccBalanceNotification(bal)));
			//_collector.emit(tuple, new Values(account.getAccBalanceNotification(bal)));
	}
	

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("balancenotification"));
	}

}
