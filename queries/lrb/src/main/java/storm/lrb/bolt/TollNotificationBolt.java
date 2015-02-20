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
import storm.lrb.model.AccidentImmutable;
import storm.lrb.model.NovLav;
import storm.lrb.model.PosReport;
import storm.lrb.model.Time;
import storm.lrb.model.VehicleInfo;
import storm.lrb.tools.StopWatch;

/**
 * This bolt calculates the actual toll for each vehicle depending on the
 * congestion and accident status of the segment the vehicle is driving on. It
 * can process streams containing position reports, accident information and
 * novLavs.
 */
/*
internal implementation notes:
- @TODO: reevaluate the need to pass the stream ids in the constructor
*/
public class TollNotificationBolt extends BaseRichBolt {

    private static final long serialVersionUID = 5537727428628598519L;
    private static final Logger LOG = LoggerFactory.getLogger(TollNotificationBolt.class);

    protected static final int MAX_SPEED_FOR_TOLL = 40;
    protected static final int MIN_CARS_FOR_TOLL = 50;
    private final static int DRIVE_EASY = 0;

    /**
     * Holds all lavs (avgs of preceeding minute) and novs (number of vehicles)
     * of the preceeding minute in a segment segment -> NovLav
     */
    private final Map<SegmentIdentifier, NovLav> allNovLavs;

    /**
     * holds vehicle information of all vehicles driving (vid -> vehicleinfo)
     */
    private final Map<Integer, VehicleInfo> allVehicles;

    /**
     * holds all current accidents (xsd -> Accidentinformation)
     */
    private final Map<SegmentIdentifier, AccidentImmutable> allAccidents;

	//protected BlockingQueue<Tuple> waitingList;
    private OutputCollector collector;

    private String tmpname;
    private final String tollNotificationStreamId;
    private final String tollAssessmentStreamId;
    private final StopWatch timer;

    public TollNotificationBolt(StopWatch timer,
            String tollNotificationStreamId,
            String tollAssessmentStreamId) {
        allNovLavs = new HashMap<SegmentIdentifier, NovLav>();
        allVehicles = new HashMap<Integer, VehicleInfo>();
        allAccidents = new HashMap<SegmentIdentifier, AccidentImmutable>();
        //waitingList = new LinkedBlockingQueue<Tuple>();
        this.timer = timer;
        this.tollNotificationStreamId = tollNotificationStreamId;
        this.tollAssessmentStreamId = tollAssessmentStreamId;
    }

    @Override
    public void prepare(@SuppressWarnings("rawtypes") Map stormConf, 
            TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        tmpname = context.getThisComponentId() + context.getThisTaskId();
        LOG.info("Component '%s' subscribed to: %s", tmpname, context.getThisSources().keySet().toString());
    }

    @Override
    public void execute(Tuple tuple) {
        if (tuple.contains(TopologyControl.POS_REPORT_FIELD_NAME)) {
            calcTollAndEmit(tuple);

        } else if (tuple.contains(TopologyControl.LAST_AVERAGE_SPEED_FIELD_NAME)) {
            updateNovLavs(tuple);

        } else if (tuple.contains(TopologyControl.ACCIDENT_INFO_FIELD_NAME)) {
            updateAccidents(tuple);
        }

        collector.ack(tuple);
    }

    private void updateNovLavs(Tuple tuple) {
        int xWay = tuple.getIntegerByField(TopologyControl.XWAY_FIELD_NAME);
        int seg = tuple.getIntegerByField(TopologyControl.SEGMENT_FIELD_NAME);
        int dir = tuple.getIntegerByField(TopologyControl.DIRECTION_FIELD_NAME);
        SegmentIdentifier segmentIdentifier = new SegmentIdentifier(xWay, seg, dir);
        int min = tuple.getIntegerByField(TopologyControl.MINUTE_FIELD_NAME);

        NovLav novlav = new NovLav(tuple.getIntegerByField(TopologyControl.NUMBER_OF_VEHICLES_FIELD_NAME),
                tuple.getDoubleByField(TopologyControl.LAST_AVERAGE_SPEED_FIELD_NAME),
                tuple.getIntegerByField(TopologyControl.MINUTE_FIELD_NAME));

        allNovLavs.put(segmentIdentifier, novlav);
        //currentNovLavMinutes.put(tuple.getStringByField("xsd"), min);
        if (LOG.isDebugEnabled()) {
            LOG.debug("updated novlavs for xway %d, segment %d, direction %d; novlav: %s",
                    xWay, seg, dir, 
                    novlav);
        }
    }

    private void updateAccidents(Tuple tuple) {
        int xway = tuple.getIntegerByField(TopologyControl.XWAY_FIELD_NAME);
        int seg = tuple.getIntegerByField(TopologyControl.SEGMENT_FIELD_NAME);
        int dir = tuple.getIntegerByField(TopologyControl.DIRECTION_FIELD_NAME);
        SegmentIdentifier xsd = new SegmentIdentifier(xway, seg, dir);

        AccidentImmutable info = (AccidentImmutable) tuple.getValueByField(TopologyControl.ACCIDENT_INFO_FIELD_NAME);
        if (LOG.isDebugEnabled()) {
            LOG.debug("received accident info: %s", info);
        }

        if (info.isOver()) {
            allAccidents.remove(xsd);
            if (LOG.isDebugEnabled()) {
                LOG.debug("TOLLN:: removed accident: " + info);
            }
        } else {
            AccidentImmutable prev = allAccidents.put(xsd, info);
            if (prev != null) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("TOLLN: accident (prev: " + prev + ")");
                } else {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("TOLLN:: added new accident");
                    }
                }
            }
        }
    }

    void calcTollAndEmit(Tuple tuple) {

        PosReport pos = (PosReport) tuple.getValueByField("PosReport");
        SegmentIdentifier segmentTriple = new SegmentIdentifier(
                pos.getSegmentIdentifier().getxWay(),
                pos.getSegmentIdentifier().getSegment(),
                pos.getSegmentIdentifier().getDirection());

        if (assessTollAndCheckIfTollNotificationRequired(pos)) {

            int toll = calcToll(segmentTriple, Time.getMinute(pos.getTime()));
            double lav = 0.0;
            int nov = 0;
            if (allNovLavs.containsKey(segmentTriple)) {
                lav = allNovLavs.get(segmentTriple).getLav();
                nov = allNovLavs.get(segmentTriple).getNov();
            }

            String notification = allVehicles.get(pos.getVehicleIdentifier()).getTollNotification(lav, toll, nov);
            if (LOG.isDebugEnabled() && toll > 0) {
                LOG.debug("TOLLN: actually calculated toll (=" + toll + ") for vid=" + pos.getVehicleIdentifier() + " at " + pos.getTime() + " sec");
            }

            if (notification.isEmpty()) {
                LOG.info("TOLLN: duplicate toll:" + allVehicles.get(pos.getVehicleIdentifier()).toString());

            } else {
                collector.emit(tollNotificationStreamId, new Values(notification));
            }

        }
    }

    /**
     * Calculate the toll amount according to the current congestion of the
     * segment
     *
     * @param position
     * @param minute
     * @return toll amount to charge the vehicle with
     */
    protected int calcToll(SegmentIdentifier position, int minute) {
        int toll = DRIVE_EASY;
        int nov = 0;
        if (allNovLavs.containsKey(position)) {
            int novLavMin = allNovLavs.get(position).getMinute();
            if (novLavMin == minute || novLavMin + 1 == minute) {
                nov = allNovLavs.get(position).getNov();
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("The novLav is not available or up to date: "
                            + allNovLavs.get(position) + "current minute" + minute);
                }
            }
        }

        if (tollConditionSatisfied(position, minute)) {
            toll = (int) (2 * Math.pow(nov - 50, 2));
        }

        return toll;
    }

    /**
     * Assess toll of previous notification and check if tollnotification is
     * required and in the course update the vehilces information
     *
     * @param posReport
     * @return true if tollnotification is required
     */
    protected boolean assessTollAndCheckIfTollNotificationRequired(PosReport posReport) {
        boolean segmentChanged;
        if (!allVehicles.containsKey(posReport.getVehicleIdentifier())) {
            segmentChanged = true;
            allVehicles.put(posReport.getVehicleIdentifier(), new VehicleInfo(posReport));
        } else {
            VehicleInfo vehicle = allVehicles.get(posReport.getVehicleIdentifier());
            SegmentIdentifier oldPosition = vehicle.getSegmentIdentifier();
            SegmentIdentifier newPosition = posReport.getSegmentIdentifier();
            segmentChanged = !oldPosition.equals(newPosition);
            if (LOG.isDebugEnabled()) {
                LOG.debug("assess toll:" + vehicle);
            }
            //assess previous toll by emitting toll info to be processed by accountbaancebolt
            collector.emit(tollAssessmentStreamId, new Values(posReport.getVehicleIdentifier(), vehicle.getXway(), vehicle.getToll(), posReport));

            vehicle.updateInfo(posReport);
        }

        return segmentChanged && !posReport.isOnExitLane();
    }

    /**
     * Check if the condition for charging toll, which depends on the minute and
     * segment of the vehicle, are given
     *
     * @param segment
     * @param minute
     * @return
     */
    protected boolean tollConditionSatisfied(SegmentIdentifier segment, int minute) {
        double segmentSpeed = 0;
        int carsOnSegment = 0;
        if (allNovLavs.containsKey(segment) && allNovLavs.get(segment).getMinute() == minute) {
            segmentSpeed = allNovLavs.get(segment).getLav();
            carsOnSegment = allNovLavs.get(segment).getNov();
        }

        boolean isAccident = false;
        if (allAccidents.containsKey(segment)) {
            isAccident = allAccidents.get(segment).active(minute);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug(segment + " => segmentSpeed: " + segmentSpeed + "\tcarsOnSegment: " + carsOnSegment);
        }

        return segmentSpeed < MAX_SPEED_FOR_TOLL && carsOnSegment > MIN_CARS_FOR_TOLL && !isAccident;
    }

    @Override
    public void cleanup() {
        timer.stop();
        LOG.debug("TollNotificationBolt was running for %d seconds",
                timer.getElapsedTimeSecs());
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

        declarer.declareStream(tollAssessmentStreamId, 
                new Fields(TopologyControl.VEHICLE_ID_FIELD_NAME,
                        TopologyControl.XWAY_FIELD_NAME,
                        TopologyControl.TOLL_ASSESSED_FIELD_NAME,
                        TopologyControl.POS_REPORT_FIELD_NAME));

        declarer.declareStream(tollNotificationStreamId, 
                new Fields(TopologyControl.TOLL_NOTIFICATION_FIELD_NAME));

    }

}
