package storm.lrb.model;

/*
 * #%L
 * lrb
 * $Id:$
 * $HeadURL:$
 * %%
 * Copyright (C) 2014 - 2015 Humboldt-Universität zu Berlin
 * %%
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
 * #L%
 */

import java.io.Serializable;
import java.util.HashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.lrb.bolt.SegmentIdentifier;
import storm.lrb.tools.Constants;

public class Accident implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(Accident.class);
    private int startTime;
    private int startMinute;
    private int endMinute;
    /**
     * timestamp of last positionreport indicating that the accident is still
     * active
     */
    private volatile int lastUpdateTime = Integer.MAX_VALUE - 1;
    private int position = -1;
    private boolean over = false;
    private final HashSet<SegmentIdentifier> involvedSegs = new HashSet<SegmentIdentifier>();
    private final HashSet<Integer> involvedCars = new HashSet<Integer>();
	//private int maxPos = -1;
    //private int minPos = -1;

    public Accident() {

    }

    public Accident(PosReport report) {
        startTime = report.getTime();
        startMinute = Time.getMinute(startTime);
        position = report.getPosition();
        lastUpdateTime = report.getTime();
        assignSegments(report.getSegmentIdentifier().getxWay(), report.getPosition(), report.getSegmentIdentifier().getDirection());
    }

    /**
     * assigns segments to accidents according to LRB req. (within 5 segments
     * upstream)
     *
     * @param xway of accident
     * @param pos of accident
     * @param dir of accident
     */
    private void assignSegments(int xway, int pos, int dir) {

        int segment = pos / Constants.MAX_NUMBER_OF_POSITIONS;

        if (dir == 0) {
            //maxPos = pos; minPos = (segment-4)*5280;
            for (int i = segment; 0 < i && i > segment - 5; i--) {
                SegmentIdentifier segmentTriple
                        = new SegmentIdentifier(xway, i, dir);
                involvedSegs.add(segmentTriple);
            }
        } else {
            //minPos = pos; maxPos = (segment+5)*5280-1;
            for (int i = segment; i < segment + 5 && i < 100; i++) {
                SegmentIdentifier segmentTriple = new SegmentIdentifier(xway, i, dir);
                involvedSegs.add(segmentTriple);
            }
        }

        LOG.debug("ACC:: assigned segments to accident: " + involvedSegs.toString());
    }

    public HashSet<SegmentIdentifier> getInvolvedSegs() {
        return involvedSegs;
    }

    protected void recordUpdateTime(int curTime) {
        this.lastUpdateTime = curTime;
    }

    public boolean active(int minute) {
        return (minute > startTime / 60 && minute <= (Time.getMinute(lastUpdateTime)));
    }

    /**
     * Checks if accident
     *
     * @param minute
     * @return
     */
    public boolean over(int minute) {
        return (minute > (lastUpdateTime + 1));
    }

    /**
     * update accident information (includes updating time and adding vid of
     * current position report if not already present
     *
     * @param report positionreport of accident car
     */
    public void updateAccident(PosReport report) {
        //add car id to involved cars if not there yet
        this.involvedCars.add(report.getVehicleIdentifier());
        this.lastUpdateTime = report.getTime();

    }

    /**
     * add car ids of involved cars to accident info
     *
     * @param vehicles hashset wtih vids
     */
    public void addAccVehicles(HashSet<Integer> vehicles) {
        this.involvedCars.addAll(vehicles);
    }

    /**
     * get all vids of vehicles involved in that accident
     *
     * @return vehicles hashset wtih vids
     */
    public HashSet<Integer> getInvolvedCars() {
        return involvedCars;
    }

    /**
     * get accident position
     *
     * @return position number
     */
    public int getAccidentPosition() {
        return position;
    }

    /*private boolean isInFrontAcc(Integer pos) {
     if( minPos<=  pos && pos <= maxPos)
     return false;
     else return true;
     }*/
    public void setOver() {
        over = true;
    }

    public void setOver(int timeinseconds) {
        endMinute = Time.getMinute(timeinseconds);
        over = true;
    }

    public boolean isOver() {
        return over;
    }

    public int getStartTime() {
        return startTime;
    }

    public int getLastUpdateTime() {
        return lastUpdateTime;
    }

    @Override
    public String toString() {
        return "Accident [startTime=" + startTime + ", startMinute="
                + startMinute + ", endMinute=" + endMinute
                + ", lastUpdateTime=" + lastUpdateTime + ", position="
                + position + ", over=" + over + ", involvedSegs="
                + involvedSegs + ", involvedCars=" + involvedCars + "]";
        //", maxPos=" + maxPos + ", minPos=" + minPos + "]";
    }

}
