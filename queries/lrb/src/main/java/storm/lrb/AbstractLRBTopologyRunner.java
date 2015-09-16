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
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import de.hub.cs.dbis.aeolus.spouts.AbstractOrderedFileInputSpout;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.Locale;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;





/**
 * 
 * @author richter
 */
public abstract class AbstractLRBTopologyRunner {
	private static final Logger LOGGER = LoggerFactory.getLogger(AbstractLRBTopologyRunner.class);
	/**
	 * Some runners need to adjust their locale, some at multiple places, so there's a need to share the information
	 * which locale to use.
	 */
	private Locale locale;
	
	/**
	 * Create an {@code AbstractLRBTopologyRunner} with locale {@code en_US.utf-8} in order to minimize the need for
	 * intertionalization.
	 */
	public AbstractLRBTopologyRunner() {
		this(new Locale("en", "US", "utf-8"));
	}
	
	/**
	 * 
	 * @param locale
	 *            the {@link Locale} returned by {@link #getLocale() } to be used in subclasses
	 */
	public AbstractLRBTopologyRunner(Locale locale) {
		if(locale == null) {
			throw new IllegalArgumentException("locale mustn't be null");
		}
		this.locale = locale;
	}
	
	public Locale getLocale() {
		return locale;
	}
	
	/**
	 * 
	 * @param offset
	 * @param executors
	 * @param xways
	 * @param host
	 * @param port
	 * @param histFile
	 *            the file to read data for historical queries from
	 * @param outputDirFile
	 *            the output directory for files created by file writer bolts
	 * @param tasks
	 * @param stormConfigDebug
	 * @param workers
	 * @param topologyName
	 *            the name of the passed {@code topology}
	 * @param topology
	 * @param runtimeMillis
	 * @param conf
	 * @throws AlreadyAliveException
	 * @throws InvalidTopologyException
	 * @throws FileNotFoundException
	 */
	public void run(StormTopology topology, int offset, int executors, int xways, String host, int port, String histFile, File outputDirFile, int tasks, boolean stormConfigDebug, int workers, String topologyName, int runtimeMillis, Config conf)
		throws AlreadyAliveException, InvalidTopologyException, FileNotFoundException {
		conf.setDebug(stormConfigDebug);
		conf.put(AbstractOrderedFileInputSpout.INPUT_FILE_NAME,
			LRBTopologyRunnerLocal.class.getResource("/datafile20seconds.dat").getFile());
		
		LOGGER.debug("starting cluster: " + "stormlrb" + topologyName);
		submit(topology, conf, runtimeMillis, workers, topologyName);
	}
	
	/**
	 * handles the submission to the cluster setup, configured and cleaned up by implementations. Called in
	 * {@link #run(backtype.storm.generated.StormTopology, int, int, int, java.lang.String, int, java.lang.String, java.lang.String, int, boolean, int, java.lang.String, int, backtype.storm.Config) }
	 * 
	 * @param topology
	 * @param conf
	 * @param runtimeMillis
	 * @param workers
	 * @param topologyName
	 */
	public abstract void submit(StormTopology topology, Config conf, int runtimeMillis, int workers, String topologyName);
}
