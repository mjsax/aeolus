package storm.lrb.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.lrb.TopologyControl;
import storm.lrb.model.AccBalRequest;
import storm.lrb.model.PosReport;
import storm.lrb.model.VehicleAccount;

/**
 *
 * This bolt recieves the toll values assesed by the tollnotificationbolt and
 * answers to account balance queries.
 *
 */
public class AccountBalanceBolt extends BaseRichBolt {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(AccountBalanceBolt.class);
    
    private OutputCollector collector;

    /**
     * Contains all vehicles and the accountinformation of the current day.
     */
    private Map<Integer, VehicleAccount> allVehicles;

    @Override
    public void prepare(@SuppressWarnings("rawtypes") Map conf, 
            TopologyContext context,
            OutputCollector collector) {
        this.collector = collector;
        allVehicles = new HashMap<Integer, VehicleAccount>();
    }

    @Override
    public void execute(Tuple tuple) {

        Fields fields = tuple.getFields();

        if (fields.contains(TopologyControl.ACCOUNT_BALANCE_REQUEST_FIELD_NAME)) {
            getBalanceAndSend(tuple);
        }

        if (tuple.contains(TopologyControl.TOLL_ASSESSED_FIELD_NAME)) {

            updateBalance(tuple);
        }

        collector.ack(tuple);
    }

    private void updateBalance(Tuple tuple) {
        Integer vid = tuple.getIntegerByField(TopologyControl.VEHICLE_ID_FIELD_NAME);
        VehicleAccount account = allVehicles.get(vid);
        PosReport pos = (PosReport) tuple.getValueByField(TopologyControl.POS_REPORT_FIELD_NAME);
        if (account == null) {
            account = new VehicleAccount(tuple.getIntegerByField(TopologyControl.TOLL_ASSESSED_FIELD_NAME), pos);
            allVehicles.put(tuple.getIntegerByField(TopologyControl.VEHICLE_ID_FIELD_NAME), account);
        } else {
            account.assessToll(tuple.getIntegerByField(TopologyControl.TOLL_ASSESSED_FIELD_NAME), pos.getEmitTime());
        }
    }

    private void getBalanceAndSend(Tuple tuple) {
        AccBalRequest bal = (AccBalRequest) tuple.getValueByField(TopologyControl.ACCOUNT_BALANCE_REQUEST_FIELD_NAME);
        VehicleAccount account = allVehicles.get(bal.getVehicleIdentifier());

        if (account == null) {
            LOG.debug("No account information available yet: at:" + bal.getTime() + " for request" + bal);
            String notification = "2," + bal.getTime() + "," + bal.getEmitTime() + "," + bal.getQueryIdentifier() + "," + 0 + "," + 0 + "###" + bal.toString() + "###";
            collector.emit(new Values(notification));
        } else {
            collector.emit(new Values(account.getAccBalanceNotification(bal)));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(TopologyControl.BALANCE_NOTIFICATION_REQUESTS_FIELD_NAME));
    }

}
