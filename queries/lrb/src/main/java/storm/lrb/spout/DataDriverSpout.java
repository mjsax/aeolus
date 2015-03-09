package storm.lrb.spout;

/*
 * #%L
 * lrb
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

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Timer;
import java.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.lrb.TopologyControl;
import storm.lrb.tools.StopWatch;

/**
 * This is a quick fix trying to overcome limitations of the actual datadriver
 * by reading directly from file and emiting to topology this is not functioning
 * yet and was mainly not finished because no distributed filesystem was
 * available (at short notice)..
 *
 */
/*
 internal implementation notes:
 - saving a stream reference rather than a filename reference doesn't work 
 because the field which are needed in storm methods (nextTuple, etc.) need to 
 be serializable and there's no such thing as a serializable stream
 - there's no usecase for reading compressed data because it'd just slow down 
 things (if any abstract this class)
 */
public class DataDriverSpout extends BaseRichSpout {

    private static final long serialVersionUID = 1L;
    private final static Logger LOGGER = LoggerFactory.getLogger(DataDriverSpout.class);

    public static String transformTuple(String line) {
        if (line == null) {
            return "";
        }
        String retValue = line;
        if (line.startsWith("#(")) {
            retValue = retValue.replace("#(", "");
            retValue = retValue.replace(")", "");
            retValue = retValue.replaceAll(" ", ",");
        }
        return retValue;
    }

    // client sends a "GET"
    private boolean firstStart = true; // to set offset

    private final int bufferInSeconds = 10; // (+1)
    private int readSecond = 0; // runner second for reading
    private int sendSecond = 0; // runner second for sending
    private final int queueBuffer = 10; // buffertime till server stops, if no
    private BufferedReader in;
    private int offset = 0;

    private final Map<Integer, Queue<String>> bufferlist = new HashMap<Integer, Queue<String>>();

    private final StopWatch stopWatch = new StopWatch();

    private Timer timer;

    private SpoutOutputCollector collector;
    private long tupleCnt = 0;
    private String filename;

    /**
     * Create a DataDriverSpout which reads from the file denoted by
     * {@code filename}
     *
     * @param filename of input data
     * @throws java.io.FileNotFoundException if the file denoted by
     * {@code filename} is not found
     */
    public DataDriverSpout(String filename) throws FileNotFoundException {
        if (filename == null || filename.isEmpty()) {
            throw new IllegalArgumentException("filename mustn't be null or empty");
        }
        this.filename = filename;
    }

    /**
     * Inits the Queue at the first start
     *
     * @return If initialization was successful
     */
    private boolean initQueue() {
        String newSecondfirst = "";
        try {
            Queue<String> myQ = new LinkedList<String>();
            // Reader ready? && actual read second < queueBuffer?
            while (in.ready() && readSecond <= queueBuffer + stopWatch.getElapsedTimeSecs()) {
                String line = in.readLine();

                line = transformTuple(line);
                String tupel[] = line.split(",");

                if (firstStart == true) {
                    firstStart = false;
                    offset = Integer.parseInt(tupel[1]);
                    if (offset > 0) {
                        LOGGER.debug("Simulation starts with offset of: " + offset);
                        stopWatch.setOffset(offset);
                    }
                }

                //add second from last round
                while (!newSecondfirst.isEmpty()) {
                    myQ.add(newSecondfirst);
                    newSecondfirst = "";
                }
                // Same second
                if (Integer.parseInt(tupel[1]) == (offset + readSecond)) {
                    myQ.add(line);
                } else { // new second, prepare last tupel of new second
                    newSecondfirst = line;
                    bufferlist.put(readSecond, myQ);
                    readSecond++;
                    myQ = new LinkedList<String>();
                }
            }
        } catch (IOException e) {
            LOGGER.warn("exception during queue initialization occured, aborting", e);
            return false;
        } catch (NumberFormatException e) {
            LOGGER.warn("exception during queue initialization occured, aborting", e);
            return false;
        }

        return true;
    }

    @Override
    public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        try {
            FileInputStream fileinput;

            fileinput = new FileInputStream(filename);

            in = new BufferedReader(new InputStreamReader(fileinput));

            if (initQueue()) {
                LOGGER.debug("Queue successfully initialized with a buffer of "
                        + bufferInSeconds + " seconds.");
                LOGGER.debug("Bufferlist: " + bufferlist.get(0).toString());
            }

            stopWatch.start(offset);

            // Timer to refill Queue each second
            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    timer = new Timer();
                    timer.schedule(new FillBufferQueue(), 2000, 1000);
                }
            });
            t.start();

        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void nextTuple() {

        try {

            if ((sendSecond <= stopWatch.getElapsedTimeSecs())) {
                Queue<String> myQ = bufferlist.get(sendSecond);

                while (myQ != null && myQ.size() > 0) {
                    if (tupleCnt % 100 == 0) {
                        LOGGER.debug(myQ.peek() + " at" + stopWatch.getElapsedTimeSecs());
                    }
                    collector.emit("stream", new Values(myQ.poll(), stopWatch));//,tupleCnt);
                    tupleCnt++;
                }
                LOGGER.debug("queue empty wait for next second?" + bufferlist.keySet());
                //queue empty go to next second
                sendSecond++;
            } else {
                int sleepMillis = (int) ((sendSecond - stopWatch.getElapsedTimeSecs()) * 100);
                LOGGER.debug("waiting %d millis for next second", sleepMillis);
                Utils.sleep(sleepMillis);
            }

        } catch (IndexOutOfBoundsException ie) {
            LOGGER.debug("BufferSize: " + bufferlist.keySet() + " SendSecond " + sendSecond, ie);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(TopologyControl.SPOUT_STREAM_ID,
                new Fields(
                        TopologyControl.TUPLE_FIELD_NAME,
                        TopologyControl.TIMER_FIELD_NAME));
    }

    private class FillBufferQueue extends TimerTask {

        private final Logger LOGGER = LoggerFactory.getLogger(FillBufferQueue.class);
        private String newSecondfirst = "";
        private int lastSecDeleted = -1;
        private final StopWatch checktime = new StopWatch();

        // Filling up the Queue, for x seconds in the future (queueBuffer
        @Override
        public void run() {
            checktime.start();

            try {

                while (!stopWatch.isRunning()) {
                    LOGGER.debug("FillBufferQueue sleeping");
                    Utils.sleep(1000);
                }

                LOGGER.debug("Sim starting fill bufferqueue at::" + stopWatch);
                //test = (int) (DataDriverSpout.stw.getElapsedTimeSecs() + DataDriverSpout.bufferInSeconds);

                Queue<String> myQ = new LinkedList<String>();

                if (in.ready() && stopWatch.isRunning()) {// && TCPDataFeeder2.readSecond <=
                    int test = sendSecond + offset;							// TCPDataFeeder2.stw.getElapsedTimeSecs()){
                    if (stopWatch.getElapsedTimeSecs() > 0 && test < stopWatch.getElapsedTimeSecs() + 20) {
                        LOGGER.debug("Too slow with output at "
                                + stopWatch.getElapsedTimeSecs() + "while sendsecond is" + test);
                    } else {

                        //delete old stuff
                        for (int i = lastSecDeleted; i < sendSecond; i++) {
                            if (bufferlist.containsKey(i)
                                    && bufferlist.get(i).isEmpty()) {
                                bufferlist.remove(i);
                                lastSecDeleted = i;
                            }
                        }

                        //fill it up so that we have at least a buffer of 5 seconds
                        while (in.ready() && readSecond <= queueBuffer + stopWatch.getElapsedTimeSecs()) {
                            String line = in.readLine();
                            if (line != null) {
                                //in case the input file is fro uppsalla
                                line = DataDriverSpout.transformTuple(line);
                                String tupel[] = line.split(",");
                                // Add last tupel of new second
                                while (!newSecondfirst.isEmpty()) {
                                    myQ.add(newSecondfirst);
                                    newSecondfirst = "";
                                }

                                // Same second
                                if (Integer.parseInt(tupel[1]) == (offset + readSecond)) {
                                    myQ.add(line);
                                    LOGGER.debug("added tuples for minute:" + readSecond);
                                } else { // new second, prepare last tupel of new second

                                    newSecondfirst = line;
                                    bufferlist.put(readSecond, myQ);
                                    readSecond++;
                                    myQ = new LinkedList<String>();

                                }
                            } else {
                                int cursec = offset + readSecond;
                                //LOGGER.debug("Last Line Added! at"+cursec);
                                this.cancel(); //Not necessary because we call System.exit
                                //System.exit(0);
                            }
                        }
                    }
                }

                LOGGER.debug(checktime.getDurationTimeSecs() + " to bufferup");

            } catch (IOException e) {
                LOGGER.warn("exception during buffer fill, skipping", e);
            } catch (NumberFormatException e) {
                LOGGER.warn("exception during buffer fill, skipping", e);
            }
        }

    }

}
