package storm.lrb.tools;

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

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CSVReader {

    private final static Logger LOGGER = LoggerFactory.getLogger(CSVReader.class);

    public CSVReader() {
    }

    public Map<Integer, Map<String, Integer>> parseFile(String csvFile) {

        Map<Integer, Map<String, Integer>> content
                = new HashMap<Integer, Map<String, Integer>>();
        BufferedReader br = null;

        try {

            br = new BufferedReader(new FileReader(csvFile));
            String line;
            String cvsSplitBy = ",";

            int cnt = 0;
            while ((line = br.readLine()) != null) {

                if (cnt == 0) {
                    LOGGER.debug("Reading histfile line: %s", line);
                }
                cnt++;
                // use comma as separator
                String[] histdata = line.split(cvsSplitBy);
                if (histdata.length != 4) {
                    return null;
                }

                Integer vid = new Integer(histdata[0]);
                Integer day = new Integer(histdata[1]);
                Integer xway = new Integer(histdata[2]);
                String key = xway.toString() + "-" + day.toString();

                Integer toll = new Integer(histdata[3]);

                if (content.containsKey(vid)) {
                    content.get(vid).put(key, toll);
                } else {
                    HashMap<String, Integer> tmp = new HashMap<String, Integer>();
                    tmp.put(key, toll);
                    content.put(vid, tmp);
                }

            }

            cnt = 0;
            for (Map.Entry<Integer, Map<String, Integer>> entry : content.entrySet()) {

                if (cnt % 50 == 0) {
                    LOGGER.debug("Toll for vid: %d" + entry.getKey());
                }

                for (Map.Entry<String, Integer> toll : entry.getValue().entrySet()) {
                    if (cnt % 7 == 0) {
                        LOGGER.debug("[key=%d, toll=%d]", toll.getKey(), toll.getValue());
                    }

                }

            }

        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    LOGGER.error("IOException occured when trying to close input stream, ignoring", e);
                    return null;
                }
            }
        }
        return content;
    }
}
