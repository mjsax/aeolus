package storm.lrb.bolt;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import storm.lrb.model.AccBalRequest;
import storm.lrb.model.DaiExpRequest;
import storm.lrb.model.PosReport;
import storm.lrb.model.TTEstRequest;
import storm.lrb.tools.CSVReader;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * Stub for daily expenditure queries. TODO either use external distributed
 * database to keep historic data or load it into memory
 * 
 */
public class DailyExpenditureBolt extends BaseRichBolt {
	
	private static final long serialVersionUID = 1L;
	OutputCollector _collector;

	private String histFile;
	
	private HashMap<Integer, HashMap<String, Integer>> tollAccounts = new HashMap<Integer, HashMap<String, Integer>>();;
	private static final Logger LOG = Logger
			.getLogger(DailyExpenditureBolt.class);

	
	@Override
	public void prepare(Map conf, TopologyContext context,OutputCollector collector) {
		_collector = collector;
		if(histFile.isEmpty())
			LOG.warn("no filename for historic data given.");
		
		//TODO read histfile with csvreader or connect to db
		
	}
	
	 public DailyExpenditureBolt(String file) {
		   histFile = file;
		  }


	@Override
	public void execute(Tuple tuple) {

		Fields fields = tuple.getFields();
		
		if (fields.contains("DaiExpRequests")) {
			
			
			DaiExpRequest exp = (DaiExpRequest) tuple.getValueByField("DaiExpRequests");
			String out = "";
			if(tollAccounts.containsKey(exp.getVid())){
				System.out.println("ExpenditureRequest: found vid");
				String key = exp.getXway().toString() + "-"+ exp.getDay().toString();
				
				int toll = tollAccounts.get(exp.getVid()).get(key);
				
				out = "3,"+exp.getTime()+","+exp.getEmitTime()+","+exp.getQid()+","+toll;
				
			}else
				out = "3,"+exp.getTime()+","+exp.getEmitTime()+","+exp.getQid()+","+"20";
			//_collector.emit(tuple, new Values(out));
			_collector.emit(new Values(out));
		}
		_collector.ack(tuple);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("expenditurenotification"));
	}


}
