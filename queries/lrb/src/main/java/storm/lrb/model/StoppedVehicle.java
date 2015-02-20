package storm.lrb.model;

import java.io.Serializable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class to count stops of a vehicle.
 */
public class StoppedVehicle implements Serializable {

    private static final Logger LOG = LoggerFactory
            .getLogger(StoppedVehicle.class);

    private static final long serialVersionUID = 1L;
    private final int vid;
    private int cnt = 0;
    private int time;
    private PosReport firstReport;
    private int position;

    //for serialization only
    public StoppedVehicle() {
        this.time = -1;
        this.vid = -1;
        this.firstReport = null;
        this.position = -1;

    }

    public StoppedVehicle(PosReport report) {
        this.vid = report.getVehicleIdentifier();
        this.time = report.getTime();
        this.firstReport = report;
        this.position = report.getPosition();
    }

    public int getPosition() {
        return position;
    }

    /**
     * checks if still stopped and records the stop and returns the number of
     * how many consecutive stops this car has reported
     *
     * @param curReport
     * @return the number of consecutives stops this vehicle reported
     */
    public int recordStop(PosReport curReport) {
        if (!stillStopped(curReport)) {
            cnt = 1;
            firstReport = curReport;
            this.time = curReport.getTime();
            this.firstReport = curReport;
            this.position = curReport.getPosition();
        } else {
            cnt++;
        }

        return cnt;
    }

    /**
     * check if new position report of car indicates that the car is still
     * stopped
     *
     * @param pos new position report
     * @return true if
     */
    protected boolean stillStopped(PosReport pos) {

        if (!(pos instanceof PosReport)) {
            return false;
        }
        if (pos.getTime() == firstReport.getTime()) {
            //LOG.info("identischer posreport");
            return true;
        } else {
            return (pos.getSegmentIdentifier().getxWay() == firstReport.getSegmentIdentifier().getxWay()
                    && pos.getSegmentIdentifier().getDirection() == firstReport.getSegmentIdentifier().getDirection())
                    && pos.getPosition().equals(firstReport.getPosition()) && pos.getTime() <= ((30 * cnt) + firstReport.getTime());
        }
    }

    public int getVid() {
        return vid;
    }

    @Override
    public String toString() {
        return "StoppedVehicle [vid=" + vid + ", cnt=" + cnt + ", time=" + time
                + ", firstReport=" + firstReport + "]";
    }

}
