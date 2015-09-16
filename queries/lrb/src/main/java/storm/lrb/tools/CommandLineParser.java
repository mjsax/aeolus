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
package storm.lrb.tools;

import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import java.util.ArrayList;
import java.util.List;

import com.beust.jcommander.Parameter;





/**
 * helper class to ease parsing of arguments
 * 
 */
public class CommandLineParser {
	
	@Parameter private final List<String> parameters = new ArrayList<String>();
	
	@Parameter(names = {"-p", "-port"}, description = "SocketPort to connect to (default: 15000)") private Integer port = 15000;
	
	@Parameter(names = {"-x", "-xway"}, description = "How many xways to process") private Integer xways = 1;
	
	@Parameter(names = {"-o", "-offset"}, description = "Offset to start simulation with") private Integer offset = 0;
	
	@Parameter(names = {"-f", "-file"}, description = "Simulation file to use") private String file = null;
	
	@Parameter(names = "-h", description = "SocketHost to connect to (default: localhost)") private String host = "localhost";
	
	@Parameter(names = "-submit", description = "Submit to Cluster") private boolean local = false;
	
	@Parameter(names = {"-hist", "-histfile"}, description = "History File to consume") private String histFile = "";
	
	@Parameter(names = {"-worker", "-w"}, description = "Number of workers") private int workers = 1;
	
	@Parameter(names = {"-executors", "-e"}, description = "Number of executors") private int executors = 1;
	
	@Parameter(names = {"-tasks", "-t"}, description = "Number of tasks") private int tasks = 3;
	
	@Parameter(names = {"-debug", "-d"}, description = "Set debug mode") private boolean debug = false;
	
	@Parameter(names = {"-n", "-name"}, description = "prefix for topology name") private String topologyNamePrefix = "";
	
	@Parameter(names = {"-r", "-runtimeMillis"}, description = "the time in milli seconds to run") private int runtimeMillis;
	
	@Parameter(names = {"-q", "-output-directory"}, description = "the output directory for file writer bolts (will be created if inexisting)") private String outputDirectory;
	
	protected void setRuntimeMillis(int runtimeMillis) {
		this.runtimeMillis = runtimeMillis;
	}
	
	public int getRuntimeMillis() {
		return this.runtimeMillis;
	}
	
	/**
	 * @return the port
	 */
	public Integer getPort() {
		return this.port;
	}
	
	/**
	 * @param port
	 *            the port to set
	 */
	protected void setPort(Integer port) {
		this.port = port;
	}
	
	/**
	 * @return the xways
	 */
	public Integer getXways() {
		return this.xways;
	}
	
	/**
	 * @param xways
	 *            the xways to set
	 */
	protected void setXways(Integer xways) {
		this.xways = xways;
	}
	
	/**
	 * @return the offset
	 */
	public Integer getOffset() {
		return this.offset;
	}
	
	/**
	 * @param offset
	 *            the offset to set
	 */
	protected void setOffset(Integer offset) {
		this.offset = offset;
	}
	
	/**
	 * @return the file
	 */
	public String getFile() {
		return this.file;
	}
	
	/**
	 * @param file
	 *            the file to set
	 */
	protected void setFile(String file) {
		this.file = file;
	}
	
	/**
	 * @return the host
	 */
	public String getHost() {
		return this.host;
	}
	
	/**
	 * @param host
	 *            the host to set
	 */
	protected void setHost(String host) {
		this.host = host;
	}
	
	/**
	 * whether the cluster ought to be run locally using {@link LocalCluster} or submitted to a remote cluster using
	 * {@link StormSubmitter#submitTopology(java.lang.String, java.util.Map, backtype.storm.generated.StormTopology) }
	 * 
	 * @return the submit
	 */
	public boolean isLocal() {
		return this.local;
	}
	
	/**
	 * @param local
	 *            the submit to set
	 */
	protected void setLocal(boolean local) {
		this.local = local;
	}
	
	/**
	 * @return the histFile
	 */
	public String getHistFile() {
		return this.histFile;
	}
	
	/**
	 * @param histFile
	 *            the histFile to set
	 */
	protected void setHistFile(String histFile) {
		this.histFile = histFile;
	}
	
	/**
	 * @return the workers
	 */
	public int getWorkers() {
		return this.workers;
	}
	
	/**
	 * @param workers
	 *            the workers to set
	 */
	protected void setWorkers(int workers) {
		this.workers = workers;
	}
	
	/**
	 * @return the executors
	 */
	public int getExecutors() {
		return this.executors;
	}
	
	/**
	 * @param executors
	 *            the executors to set
	 */
	protected void setExecutors(int executors) {
		this.executors = executors;
	}
	
	/**
	 * @return the tasks
	 */
	public int getTasks() {
		return this.tasks;
	}
	
	/**
	 * @param tasks
	 *            the tasks to set
	 */
	protected void setTasks(int tasks) {
		this.tasks = tasks;
	}
	
	/**
	 * @return the debug
	 */
	public boolean isDebug() {
		return this.debug;
	}
	
	/**
	 * @param debug
	 *            the debug to set
	 */
	protected void setDebug(boolean debug) {
		this.debug = debug;
	}
	
	/**
	 * @return the topologyNamePrefix
	 */
	public String getTopologyNamePrefix() {
		return this.topologyNamePrefix;
	}
	
	/**
	 * @param topologyNamePrefix
	 *            the topologyNamePrefix to set
	 */
	protected void setTopologyNamePrefix(String topologyNamePrefix) {
		this.topologyNamePrefix = topologyNamePrefix;
	}
	
	public String getOutputDirectory() {
		return outputDirectory;
	}
	
	public void setOutputDirectory(String outputDirectory) {
		this.outputDirectory = outputDirectory;
	}
	
}
