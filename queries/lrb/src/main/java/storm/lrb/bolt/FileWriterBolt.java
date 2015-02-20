package storm.lrb.bolt;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.lrb.tools.TupleHelpers;

/**
 * This bolt bufferwrites all recieved tuples to the given filename in the
 * STORM_LOCAL_DIR. The buffersize can be adjusted with the constructor.
 * (flushes every two minutes, this can be adjusted by changing the
 * TOPOLOGY_TICK_TUPLE_FREQ_SECS)
 */
public class FileWriterBolt extends BaseRichBolt {

    private static final long serialVersionUID = 1L;
    private final static Logger LOGGER = LoggerFactory.getLogger(FileWriterBolt.class);

    private PrintWriter printwriter;
    private Writer filewriter;
    private Writer bufferedwriter;

    private OutputCollector collector;

    private final String filename;

    private final boolean local;

    private int bufferfactor = 2;

    public FileWriterBolt(String filename, int bufferfactor) {
        this.filename = filename;
        this.local = false;
        this.bufferfactor = bufferfactor;
    }

    public FileWriterBolt(String filename, boolean local) {
        this.filename = filename;
        this.local = local;
    }

    /**
     * set the bufferfactor higher if a the rate of emitting tuples is expected
     * to be high
     *
     * @param filename
     * @param bufferfactor
     * @param local activate some buffer tweaks when the bolt runnings locally
     */
    public FileWriterBolt(String filename, int bufferfactor, boolean local) {
        this.filename = filename;
        this.local = local;
        this.bufferfactor = bufferfactor;
    }

    @Override
    public void prepare(@SuppressWarnings("rawtypes") Map conf, 
            TopologyContext topologyContext,
            OutputCollector outputCollector) {
        this.collector = outputCollector;
        printwriter = null;

        String path = (String) conf.get(Config.STORM_LOCAL_DIR);

        DateTime dt = new DateTime();
        String b = dt.toString("hh-mm-ss");

        String fileuri = path + "/" + filename + "_" + b + ".out";
        LOGGER.debug("Writing to file '%s'", fileuri);

        try {

            filewriter = new FileWriter(fileuri);
            bufferedwriter = new BufferedWriter(filewriter, bufferfactor * 1024);
            printwriter = new PrintWriter(bufferedwriter);

        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public void execute(Tuple tuple) {

        if (TupleHelpers.isTickTuple(tuple)) {

            printwriter.flush();
        } else {
            Object value = tuple.getValue(0);
            printwriter.println(value);
            collector.ack(tuple);

            if (local) {
                printwriter.flush();
            }

        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 70);
        return conf;
    }

    @Override
    public void cleanup() {
        printwriter.flush();
        printwriter.close();
        try {
            bufferedwriter.close();
            filewriter.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        super.cleanup();

    }
}
