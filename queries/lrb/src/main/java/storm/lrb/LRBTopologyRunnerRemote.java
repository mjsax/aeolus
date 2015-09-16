/*
 * #!
 * %
 * Copyright (C) 2014 - 2015 Humboldt-Universität zu Berlin
 * %
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
 * #_
 */
package storm.lrb;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;





/**
 * Responsible for running an LRB topology created by {@link LRBTopologyBuilder} (or somehow else which is not
 * recommended). Topologies are either run remote or using {@link LocalCluster} depending on whether the {@code submit}
 * flag of
 * {@link #run(int, int, int, java.lang.String, int, java.lang.String, java.lang.String, int, boolean, boolean, int, java.lang.String, int) }
 * is set.
 * 
 * @author richter
 */
public class LRBTopologyRunnerRemote extends AbstractLRBTopologyRunner {
	private static final Logger LOGGER = LoggerFactory.getLogger(LRBTopologyRunnerLocal.class);
	
	@Override
	public void submit(StormTopology topology, Config conf, int runtimeMillis, int workers, String topologyName) {
		conf.setNumWorkers(workers);
		conf.setNumAckers(workers);
		LOGGER.debug("submitting to remote cluster");
		try {
			StormSubmitter.submitTopology(topologyName, conf, topology);
		} catch(AlreadyAliveException ex) {
			throw new RuntimeException(ex);
		} catch(InvalidTopologyException ex) {
			throw new RuntimeException(ex);
		}
	}
}
