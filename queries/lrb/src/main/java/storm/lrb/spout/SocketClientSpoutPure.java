package storm.lrb.spout;

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

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.lrb.TopologyControl;
import storm.lrb.tools.StopWatch;

/**
 * This Spout connects to the given host and socket, reads tuples line by line
 * and emits it to the default stream. This spout is used to make reading from
 * socket faster by defering LRBtuple creation to the {@link DispatcherBolt}
 * which can be parallelized
 *
 */
public class SocketClientSpoutPure extends BaseRichSpout {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(SocketClientSpoutPure.class);
    private SpoutOutputCollector collector;
    private final String host;
    private final int port;
    private Socket clientSocket;
    private InputStreamReader isr;
    private BufferedReader in;
    private final StopWatch cnt;
    private long tupleCnt = 0;
    private boolean firstrun = true;

    public SocketClientSpoutPure(String host, int port) {
        this.host = host;
        this.port = port;
        this.cnt = new StopWatch();
    }

    @Override
    @SuppressWarnings("SleepWhileInLoop")
    public void open(@SuppressWarnings("rawtypes") Map conf, 
            TopologyContext context, 
            SpoutOutputCollector collector) {
        this.collector = collector;

        boolean goon = true;
        //try no more than 2 minutes to connect 
        while (cnt.getElapsedTimeSecs() <= 120) {
            try {

                if (cnt.getElapsedTimeSecs() > 2) {
                    Utils.sleep(5000);
                }

                if (goon == true) {
                    goon = false;
                } else {
                    break;
                }

                clientSocket = new Socket(InetAddress.getByName(host), port);
                LOG.info("Connection to " + host + ":" + port
                        + " established.\t StormTimer: " + cnt.getElapsedTimeSecs() + "s");

                isr = new InputStreamReader(clientSocket.getInputStream());
                in = new BufferedReader(isr);

            } catch (java.net.ConnectException e) {
                goon = true;
                LOG.warn(String.format("failed to connect to host '%s' on port %d (see following exception for details), retrying...", host, port), e);
                Utils.sleep(2000);
            } catch (UnknownHostException e) {
                throw new RuntimeException(e);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

    }

    @Override
    public void nextTuple() {
        String line = "";
        try {
            line = in.readLine();
            if (line != null) {
                if (line.startsWith("#")) {
                    return;
                }
                if (firstrun) {
                    int offset = Integer.parseInt(line.substring(2, line.indexOf(',', 2)));
                    LOG.info("Simulation starts with offset " + offset);
                    cnt.start(offset);
                    firstrun = false;
                }
                //LOG.info(line+ " at "+cnt.getElapsedTimeSecs());
                collector.emit(TopologyControl.SPOUT_STREAM_ID, new Values(line, cnt), tupleCnt);
                tupleCnt++;

            } else {
                int waitMillis = 50;
                LOG.debug("Waiting %d millis for the next tuple to arrive", waitMillis);
                Utils.sleep(waitMillis);
            }
        } catch (NumberFormatException e) {
            LOG.error("Error in line '%s'", line);
            throw new RuntimeException(e);
        } catch (IllegalArgumentException e) {
            LOG.error("Error in line '%s'", line);
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        // in.close();
        try {
            cnt.stop();
            LOG.debug("Simulation duration: %d s; # of tuples: ", 
                    cnt.getDurationTimeSecs(),
                    tupleCnt);
            isr.close();
            in.close();
            clientSocket.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.setMaxTaskParallelism(1);
        return conf;
    }

    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(TopologyControl.SPOUT_STREAM_ID, 
                new Fields(TopologyControl.TUPLE_FIELD_NAME, 
                        TopologyControl.TIMER_FIELD_NAME));
    }

}
