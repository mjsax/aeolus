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
import backtype.storm.generated.StormTopology;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Scanner;
import java.util.Set;
import org.apache.commons.lang.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;





/**
 * Runs the LRB topology on a variable number of vagrant instances.
 * 
 * There're <a href="https://github.com/guigarage/vagrant-binding">Vagrant Java bindings</a>, but they're incompatible
 * with up to date VirtualBox versions (> 4.2), requested https://github.com/guigarage/vagrant-binding/issues/6 for a
 * fix; until then configure vagrant on OS level. Currently only Linux is supported.
 * 
 * Vagrant uses bridged networks based on configurable interfaces (see
 * {@link #LRBTopologyRunnerVagrant(java.lang.String) } for details). IP addresses are either assigned with a DHCP server
 * (in this case you need to make sure that one is really present and working in your network becuase
 * {@code LRBTopologyRunnerVagrant} might behave unexpected if one is promised, but none provided) or a set of static
 * IPs (as for every static IP configuration you have to make sure there're no address collisions).
 * {@code LRBTopologyRunnerVagrant} provides no interface to figure out which IP is used by the nimbus and which worker
 * uses which IP (you might figure it out by checking the {@code Vagrantfile} in the corresponding base directories).
 * 
 * The current implementation reads the output of started processes into memory, so this might cause trouble (most
 * likely {@link OutOfMemoryError} for long running processes producing a lot of output.
 * 
 * {@code LRBTopologyRunnerVagrant} offers automatic removal of used resources (directories and files) after each
 * invokation of
 * {@link #run(backtype.storm.generated.StormTopology, int, int, int, java.lang.String, int, java.lang.String, java.io.File, int, boolean, int, java.lang.String, int, backtype.storm.Config) }
 * . The removal policy is controlled with the {@code removalPolicy} property.
 * 
 * The storage space consumption is enormous. Make sure you have 50 GB free disk space available. Due to the fact that a
 * lot of files produces by this topology runner are redundant, a deduplicating file system like btrfs or ZFS will help
 * a lot.
 * 
 * @author richter
 */
/*
 * internal implementation notes: - run... methods are private because it's not wise to expose them because this is a
 * topology runner and not a swiss army knife (in case the methods ought to be reused a level of abstraction ought to be
 * included)
 */
public class LRBTopologyRunnerVagrant extends AbstractLRBTopologyRunner {
	
	private static final Logger logger = LoggerFactory.getLogger(LRBTopologyRunnerVagrant.class);
	private final static int VAGRANT_VMS_COUNT_DEFAULT = 5;
	private int vagrantStormWorkerCount;
	private final static String BASE_DIR_PATH_DEFAULT = "lrb-topology-runner-vagrant-vms";
	private File baseDir;
	private final String vagrantBridgeIface;
	private final Set<String> vagrantBridgeStaticIPs;
	/**
	 * A constant indicating that the vagrant directory ought to be kept after usage (e.g. for debugging purposes or
	 * usage with another application).
	 */
	public final static int REMOVE_POLICY_KEEP = 1;
	/**
	 * A constant indicating that the the vagrant directory ought to be removed with a statement in the code. In case an
	 * exception occurs the statement might not be reached.
	 */
	public final static int REMOVE_POLICY_NORMAL = 2;
	/**
	 * A constant indicating that a JVM shutdown hook for removing the vagrant directory ought to be created and added
	 * using {@link Runtime#addShutdownHook(java.lang.Thread)
	 * }.
	 */
	public final static int REMOVE_POLICY_SHUTDOWN_HOOK = 4;
	private final static Set<Integer> REMOVE_POLICIES = new HashSet<Integer>(Arrays.asList(REMOVE_POLICY_KEEP,
		REMOVE_POLICY_NORMAL, REMOVE_POLICY_SHUTDOWN_HOOK));
	public final static int REMOVE_POLICY_DEFAULT = REMOVE_POLICY_NORMAL;
	private int removalPolicy = REMOVE_POLICY_DEFAULT;
	/**
	 * The major Java version to use in the vagrant VMs.
	 */
	private int javaMajorVersion = 6;
	/**
	 * the number of all vagrant VMs (nimbus, worker, etc.) running.
	 */
	private int vagrantVMCounter = 0;
	private String stormVersionString = "0.9.3";
	@SuppressWarnings("serial") private Map<String, String> stormTarballVersionChecksumMap = new HashMap<String, String>() {
		{
			put("0.10.0-beta1", "7c24eae4a23e1fffb9686cbc084fc284");
			put("0.9.3", "43353ebf96a4dc13e8712d759eb202a1");
			put("0.9.4", "012361b3da022470d215875611a01fcf");
			put("0.9.5", "2489db53a34505016e38d9861154a8a3");
		}
	};
	private boolean failOnMissingStormTarballChecksum = true;
	
	/**
	 * 
	 * @return @throws java.net.SocketException if {@link NetworkInterface#getNetworkInterfaces()
	 * } throws such an
	 *         exception
	 */
	public static List<NetworkInterface> retrieveNetworkInterfaces() throws SocketException {
		Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
		List<NetworkInterface> retValue = Collections.list(interfaces);
		return retValue;
	}
	
	/**
	 * 
	 * @return @throws java.net.SocketException if {@link #retrieveNetworkInterfaces()
	 * } throws such an exception
	 */
	public static List<String> retrieveNetworkInterfaceNames() throws SocketException {
		List<String> retValue = new LinkedList<String>();
		List<NetworkInterface> interfaces = retrieveNetworkInterfaces();
		for(NetworkInterface interface1 : interfaces) {
			retValue.add(interface1.getName());
		}
		return retValue;
	}
	
	/**
	 * Searches for matches of network interface names and names in {@code vagrantBridgeIfacePriorities} and returns the
	 * matching name with the lowest index in the list.
	 * 
	 * @param vagrantBridgeIfacePriorities
	 * @return
	 * @throws SocketException
	 */
	protected static String selectPriorityIFaceName(List<String> vagrantBridgeIfacePriorities) throws SocketException {
		List<String> interfaceNames = retrieveNetworkInterfaceNames();
		if(interfaceNames.isEmpty()) {
			throw new IllegalStateException("no network interface available");
		}
		int index = Integer.MAX_VALUE;
		String selectedIFaceName = null;
		for(String iFaceName : interfaceNames) {
			int interfaceNameIndex = -1;
			for(String iFaceRegEx : vagrantBridgeIfacePriorities) {
				if(iFaceName.matches(iFaceRegEx)) {
					interfaceNameIndex += 1;
					break;
				}
			}
			if(interfaceNameIndex == -1) {
				continue;
			}
			if(interfaceNameIndex < index) {
				selectedIFaceName = iFaceName;
			}
		}
		if(selectedIFaceName == null) {
			selectedIFaceName = interfaceNames.get(0);
		}
		return selectedIFaceName;
	}
	
	public LRBTopologyRunnerVagrant(int vagrantStormWorkerCount, File baseDir, List<String> vagrantBridgeIFacePriorities)
		throws SocketException {
		this(vagrantStormWorkerCount, baseDir, selectPriorityIFaceName(vagrantBridgeIFacePriorities), null,
			REMOVE_POLICY_DEFAULT);
	}
	
	/**
	 * 
	 * @param vagrantBridgeIFacePriorities
	 *            a list of regular expressions for network interface names used to determine the priority of one
	 *            interface over another. This is useful if you are certain that certain interface names will be
	 *            available and you prefer one or some of them over another.
	 * @throws java.net.SocketException
	 *             if {@link #retrieveNetworkInterfaceNames()
	 * } throws such an exception
	 */
	public LRBTopologyRunnerVagrant(List<String> vagrantBridgeIFacePriorities) throws SocketException {
		this(selectPriorityIFaceName(vagrantBridgeIFacePriorities));
	}
	
	public LRBTopologyRunnerVagrant(File baseDir, List<String> vagrantBridgeIFacePriorities) throws SocketException {
		this(VAGRANT_VMS_COUNT_DEFAULT, baseDir, selectPriorityIFaceName(vagrantBridgeIFacePriorities), null,
			REMOVE_POLICY_DEFAULT);
	}
	
	public LRBTopologyRunnerVagrant(File baseDir, List<String> vagrantBridgeIFacePriorities, int removePolicy)
		throws SocketException {
		this(VAGRANT_VMS_COUNT_DEFAULT, baseDir, selectPriorityIFaceName(vagrantBridgeIFacePriorities), null,
			removePolicy);
	}
	
	public LRBTopologyRunnerVagrant(int vagrantStormWorkerCount, File baseDir, String vagrantBridgeIface) {
		this(vagrantStormWorkerCount, baseDir, vagrantBridgeIface, null, REMOVE_POLICY_DEFAULT);
	}
	
	public LRBTopologyRunnerVagrant(String vagrantBridgeIface) {
		this(vagrantBridgeIface, null);
	}
	
	/**
	 * 
	 * @param vagrantBridgeIface
	 *            Vagrant needs to know which interface to base the network bridge on. Specify the network interface
	 *            identifier here.
	 * @param vagrantBridgeStaticIPs
	 *            static IPs used for vagrant VMs; if {@code null} DHCP will be used
	 */
	public LRBTopologyRunnerVagrant(String vagrantBridgeIface, Set<String> vagrantBridgeStaticIPs) {
		this(VAGRANT_VMS_COUNT_DEFAULT, new File(BASE_DIR_PATH_DEFAULT), vagrantBridgeIface, vagrantBridgeStaticIPs,
			REMOVE_POLICY_DEFAULT);
	}
	
	public LRBTopologyRunnerVagrant(int vagrantStormWorkerCount, File baseDir, String vagrantBridgeIface,
		Set<String> vagrantBridgeStaticIPs, int removePolicy) {
		if(!SystemUtils.IS_OS_LINUX) {
			throw new RuntimeException(String.format("%s is supported on Linux only", LRBTopologyRunnerVagrant.class));
		}
		this.vagrantStormWorkerCount = vagrantStormWorkerCount;
		if(baseDir.exists()) {
			if(!baseDir.isDirectory()) {
				throw new IllegalArgumentException(String.format("base directory '%s' exists and isn't a directory",
					baseDir.getAbsolutePath()));
			}
		} else {
			logger.info("creating inexisting baseDir '{}'", baseDir);
			baseDir.mkdirs();
		}
		this.baseDir = baseDir;
		this.vagrantBridgeIface = vagrantBridgeIface;
		if(vagrantBridgeStaticIPs != null && vagrantBridgeStaticIPs.size() != vagrantStormWorkerCount + 1) {
			throw new IllegalArgumentException(
				"the size of vagrantBridgeStaticIPs has to be the same as vagrantStormWorkerCount+1");
		}
		this.vagrantBridgeStaticIPs = vagrantBridgeStaticIPs;
		if(!REMOVE_POLICIES.contains(removePolicy)) {
			throw new IllegalArgumentException(String.format("remove policy %d isn't valid (has to be one of %s)",
				removePolicy, REMOVE_POLICIES));
		}
		this.removalPolicy = removePolicy;
	}
	
	@Override
	public void submit(StormTopology topology, Config conf, int runtimeMillis, int workers, String topologyName) {
		Queue<String> iPPool = null;
		if(this.vagrantBridgeStaticIPs != null) {
			iPPool = new LinkedList<String>(this.vagrantBridgeStaticIPs);
		}
		try {
			Runtime.getRuntime().exec(new String[] {"vagrant", "--version"});
		} catch(IOException ex) {
			throw new RuntimeException("vagrant binary isn't available. If "
				+ "vagrant isn't installed, please install it following "
				+ "http://docs.vagrantup.com/v2/installation/index.html " + "(see nested excpetion for details)", ex);
		}
		List<File> removalList = new LinkedList<File>();
		try {
			// Storm nimbus setup
			final File nimbusVagrantDir = createVagrantDir(baseDir, "nimbus", removalList);
			logger.debug("using '{}' as temporary directory for nimbus vagrant VM", nimbusVagrantDir);
			String nimbusIP;
			if(iPPool == null) {
				nimbusIP = vagrantUp(nimbusVagrantDir, null // ipStatic (null indicates usage of DHCP)
				);
			} else {
				nimbusIP = vagrantUp(nimbusVagrantDir, iPPool.poll());
			}
			prepareVagrant(nimbusVagrantDir, javaMajorVersion);
			runVagrantCommand(nimbusVagrantDir, new String[] {
				"vagrant",
				"ssh",
				"-c",
				String.format("cat << EOF > apache-storm-%s/conf/storm.yaml\n" + "storm.zookeeper.servers:\n"
					+ " - \"%s\"\n" + "EOF", stormVersionString, nimbusIP)});
			
			Thread nimbusThread = new Thread() {
				@Override
				public void run() {
					try {
						runVagrantCommand(
							nimbusVagrantDir,
							new String[] {"vagrant", "ssh", "-c",
								String.format("python apache-storm-%s/bin/storm nimbus ", stormVersionString)});
					} catch(IOException ex) {
						throw new RuntimeException(ex);
					} catch(InterruptedException ex) {
						throw new RuntimeException(ex);
					}
				}
			};
			nimbusThread.start();
			// Storm workers setup
			Set<Thread> workerThreads = new HashSet<Thread>(vagrantStormWorkerCount);
			for(int i = 0; i < vagrantStormWorkerCount; i++) {
				final File workerVagrantDir = createVagrantDir(baseDir, String.format("worker-%d", i), removalList);
				logger.debug("using '{}' as temporary directory for {}st/nd/rd/th worker vagrant VM", workerVagrantDir,
					i + 1);
				String ip;
				if(iPPool == null) {
					ip = vagrantUp(workerVagrantDir, null);
				} else {
					ip = vagrantUp(workerVagrantDir, iPPool.poll());
				}
				
				prepareVagrant(workerVagrantDir, javaMajorVersion);
				runVagrantCommand(workerVagrantDir, new String[] {
					"vagrant",
					"ssh",
					"-c",
					String.format("cat << EOF > apache-storm-%s/conf/storm.yaml" + "storm.zookeeper.servers:\n"
						+ " - \"%s\"\n" + "nimbus.host: \"%s\"\n" + "EOF", stormVersionString, nimbusIP, nimbusIP)});
				Thread nimbusSupervisorThread = new Thread() {
					@Override
					public void run() {
						try {
							runVagrantCommand(
								workerVagrantDir,
								new String[] {"vagrant", "ssh", "-c",
									String.format("python apache-storm-%s/bin/storm supervisor", stormVersionString)});
							// not too elegant to keep ssh process running, but
							// this allows us to monitor output and status as easy as
							// possible
						} catch(IOException ex) {
							throw new RuntimeException(ex);
						} catch(InterruptedException ex) {
							throw new RuntimeException(ex);
						}
					}
				};
				nimbusSupervisorThread.start();
				workerThreads.add(nimbusSupervisorThread);
			}
			for(Thread workerThread : workerThreads) {
				workerThread.join();
			}
			nimbusThread.join();
		} catch(IOException ex) {
			throw new RuntimeException(ex);
		} catch(InterruptedException ex) {
			throw new RuntimeException(ex);
		}
		if(removalPolicy == REMOVE_POLICY_NORMAL) {
			for(File file : removalList) {
				logger.debug("removing file '%s' because removalPolicy is %d", file.getAbsolutePath(), removalPolicy);
				file.delete();
			}
		}
	}
	
	/**
	 * Creates a directory based on {@code baseDir} if it doesn't exist. Asserts that the directory isn't an existing
	 * file (fails with {@link IllegalArgumentException} if that's the case).
	 * 
	 * Handles all removal policies.
	 * 
	 * @param baseDir
	 * @param name
	 *            the name of the vagrant directory (most likely indicating whether it contains a nimbus or worker VM)
	 * @param removalList
	 *            a list to add the return value to (ignored if {@code removalPolicy} is !=
	 *            {@link #REMOVE_POLICY_NORMAL}
	 * @return
	 */
	protected File createVagrantDir(File baseDir, String name, List<File> removalList) {
		validateBaseDir(baseDir);
		final File vagrantDir = new File(baseDir, name);
		if(!vagrantDir.exists()) {
			vagrantDir.mkdirs();
		} else if(vagrantDir.isFile()) {
			throw new IllegalArgumentException(String.format(
				"the vagrant directory '%s' which ought to be created points to an existing file",
				vagrantDir.getAbsolutePath()));
		}
		if(removalPolicy == REMOVE_POLICY_NORMAL) {
			removalList.add(vagrantDir);
		} else if(removalPolicy == REMOVE_POLICY_SHUTDOWN_HOOK) {
			Runtime.getRuntime().addShutdownHook(new Thread() {
				@Override
				public void run() {
					logger.debug("running shutdown hook to remove vagrant directory '%s'", vagrantDir.getAbsolutePath());
					if(vagrantDir.exists()) {
						logger.debug("removing vagrant directory '%s' in shutdown hook", vagrantDir.getAbsolutePath());
						vagrantDir.delete();
					} else {
						logger.debug("vagrant directory '%s' to be removed in shutdown hook already deleted",
							vagrantDir.getAbsolutePath());
					}
				}
			});
		}
		return vagrantDir;
	}
	
	/**
	 * runs {@code cmds} in a {@link Process}, starts {@code vagrant} and catches eventual errors. Due to the fact that
	 * Java interprocess communication is rather poor, there's no way around setting the stdout and stderr of the
	 * current JVM using {@link System#setOut(java.io.PrintStream)
	 * } and {@link System#setErr(java.io.PrintStream) }. All
	 * processes are run with the current {@code locale} in order to allow control of interfactionalization.
	 * 
	 * The output to {@code stdout} will be printed sequentially, but might be (severely) delayed. The output to
	 * {@code stderr} will be printed only if an error occurs, i.e. if the process' return code is not {@code 0}.
	 * 
	 * @param vagrantDir
	 *            the working directory of the process
	 * @param cmds
	 *            the strings which make the command to execute
	 * @throws SecurityException
	 *             if {@link System#setOut(java.io.PrintStream) } or {@link System#setErr(java.io.PrintStream) } throws
	 *             one
	 * @throws IOException
	 *             if one occurs during the execution of the process (see {@link Process#} for details)
	 * @throws InterruptedException
	 *             if one occurs in {@link Process#waitFor() }
	 */
	protected void runVagrantCommand(File vagrantDir, String[] cmds) throws IOException, InterruptedException {
		validateBaseDir(vagrantDir);
		logger.debug("running '{}' in '{}'", Arrays.toString(cmds), vagrantDir);
		// unclear how to redirect stdout and stderr so that they're logged in
		// order (setting System.setOut and System.setErr simply doesn't work)
		ProcessBuilder processBuilder = new ProcessBuilder(cmds);
		processBuilder
			.directory(vagrantDir)
			.redirectError(ProcessBuilder.Redirect.PIPE)
			.environment()
			.put(
				"LC_ALL",
				String
					.format("%s_%s.%s", getLocale().getLanguage(), getLocale().getCountry(), getLocale().getVariant()));
		Process process = processBuilder.start();
		int processReturnCode = process.waitFor();
		Scanner scanStdout = new Scanner(process.getInputStream());
		while(scanStdout.hasNextLine()) {
			logger.info(scanStdout.nextLine());
		}
		if(processReturnCode != 0) {
			Scanner scanStderr = new Scanner(process.getErrorStream());
			while(scanStderr.hasNextLine()) {
				logger.error(scanStderr.nextLine());
			}
			throw new RuntimeException(
				String
					.format(
						"The vagrant command '%s' failed. The process returned with code %d. Check the preceeding output for details and reasons.",
						Arrays.toString(cmds), processReturnCode));
		}
	}
	
	/**
	 * runs {@code cmds} like {@link #runVagrantCommand(java.io.File, java.lang.String[])
	 * } would, but only prints stderr
	 * output on failure and returns stdout output.
	 * 
	 * @param vagrantDir
	 * @param cmds
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	protected String runVagrantCommandOutput(File vagrantDir, String[] cmds) throws IOException, InterruptedException {
		validateBaseDir(vagrantDir);
		ProcessBuilder processBuilder = new ProcessBuilder(cmds);
		processBuilder
			.directory(vagrantDir)
			.redirectOutput(ProcessBuilder.Redirect.PIPE)
			.environment()
			.put(
				"LC_ALL",
				String
					.format("%s_%s.%s", getLocale().getLanguage(), getLocale().getCountry(), getLocale().getVariant()));
		Process process = processBuilder.start();
		int processReturnCode = process.waitFor();
		Scanner scanStdout = new Scanner(process.getInputStream());
		StringBuilder retValueBuilder = new StringBuilder(100 // initialCapacity (can be small because we usually
		// retrieve an IP or other short strings)
		);
		while(scanStdout.hasNextLine()) {
			retValueBuilder.append(scanStdout.nextLine());
		}
		if(processReturnCode != 0) {
			Scanner scanStderr = new Scanner(process.getErrorStream());
			while(scanStderr.hasNextLine()) {
				logger.error(scanStderr.nextLine());
			}
			throw new RuntimeException();
		}
		String retValue = retValueBuilder.toString();
		return retValue;
	}
	
	protected void validateBaseDir(File baseDir) {
		if(!baseDir.exists()) {
			throw new IllegalArgumentException(String.format("directory '%s' doesn't exist", baseDir));
		} else if(!baseDir.isDirectory()) {
			throw new IllegalArgumentException(String.format("'%s' exists, but isn't a directory", baseDir));
		}
	}
	
	/**
	 * 
	 * @param vagrantDir
	 * @param staticIP
	 *            the static IP to use or indication for usage of DHCP if {@code null}
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	protected String vagrantUp(File vagrantDir, String staticIP) throws IOException, InterruptedException {
		assert vagrantDir.exists();
		assert vagrantDir.isDirectory();
		FileWriter vagrantFileWriter = new FileWriter(new File(vagrantDir, "Vagrantfile"));
		String vagrantNetworkString;
		if(staticIP == null) {
			// use DHCP
			vagrantNetworkString = String
				.format(
					"  config.vm.network \"public_network\", :bridge => \"%s\", :auto_config => \"true\", :netmask => \"255.255.255.0\"",
					vagrantBridgeIface);
			// "  config.vm.network \"public_network\", bridge: \"%s: lrb bridge iface\"", vagrantBridgeIface); doesn't
			// work
		} else {
			vagrantNetworkString = String
				.format(
					"  config.vm.network \"public_network\", :bridge => \"%s\", ip:\"%s\", :auto_config => \"false\", :netmask => \"255.255.255.0\"",
					vagrantBridgeIface, staticIP);
			// "  config.vm.network \"public_network\", bridge: \"%s: lrb bridge iface\", ip: \"%s\"",
			// vagrantBridgeIface, staticIP);
		}
		vagrantFileWriter.append(String.format("VAGRANTFILE_API_VERSION = \"2\"\n"
			+ "Vagrant.configure(VAGRANTFILE_API_VERSION) do |config|\n" + "  config.vm.box = \"ubuntu/vivid64\"\n"
			+ "%s\n" + "end", vagrantNetworkString));
		vagrantFileWriter.flush();
		vagrantFileWriter.close();
		logger.info("starting {}st/nd/rd/th vagrant VM", this.vagrantVMCounter + 1);
		runVagrantCommand(vagrantDir, new String[] {"vagrant", "up"});
		// get DHCP assigned IP (can be retrieved from inside VM only
		// <ref>https://friendsofvagrant.github.io/v1/docs/bridged_networking.html</ref>)
		String ip = runVagrantCommandOutput(vagrantDir, new String[] {"vagrant", "ssh", "-c",
			"ifconfig eth1 | grep 'inet addr' | cut -d ' ' -f 12 | cut -d ':' -f 2"});
		assert staticIP != null ? ip.equals(staticIP) : true;
		this.vagrantVMCounter += 1;
		return ip;
	}
	
	/**
	 * Retrieves all necessary files to run {@code storm} and {@code zookeeper} including those distributions
	 * themselves, except configuration files. Installs a JDK determined by {@code javaMajorVersion}.
	 * 
	 * @param vagrantDir
	 * @param javaMajorVersion
	 * @throws IOException
	 * @throws InterruptedException
	 */
	protected void prepareVagrant(File vagrantDir, int javaMajorVersion) throws IOException, InterruptedException {
		assert vagrantDir.exists();
		assert vagrantDir.isDirectory();
		assert javaMajorVersion == 6 || javaMajorVersion == 7 || javaMajorVersion == 8;
		vagrantFetchStormAndZookeeper(vagrantDir);
		runVagrantCommand(
			vagrantDir,
			new String[] {"vagrant", "ssh", "-c",
				String.format("sudo apt-get update && sudo apt-get install --yes openjdk-%d-jdk", javaMajorVersion)});
	}
	
	protected void vagrantFetchStormAndZookeeper(File vagrantDir) throws IOException, InterruptedException {
		assert vagrantDir.exists();
		assert vagrantDir.isDirectory();
		String stormTarballChecksum = stormTarballVersionChecksumMap.get(this.stormVersionString);
		if(stormTarballChecksum == null && this.failOnMissingStormTarballChecksum) {
			throw new IllegalArgumentException(String.format("no checksum mapped for storm tarball version %s",
				this.stormVersionString));
		}
		String command;
		if(stormTarballChecksum == null) {
			command = String.format(
				"wget http://mirror.23media.de/apache/zookeeper/zookeeper-3.4.6/zookeeper-3.4.6.tar.gz "
					+ "&& tar xf zookeeper-3.4.6.tar.gz; "
					+ "wget http://mirror.dkd.de/apache/storm/apache-storm-%s/apache-storm-%s.tar.gz "
					+ "&& tar xf apache-storm-%s.tar.gz", this.stormVersionString, this.stormVersionString,
				this.stormVersionString);
		} else {
			command = String.format(
				"if [ \"x`md5sum zookeeper-3.4.6.tar.gz | cut -d ' ' -f 1`\" != \"x971c379ba65714fd25dc5fe8f14e9ad1\" ]; "
					+ "then wget http://mirror.23media.de/apache/zookeeper/zookeeper-3.4.6/zookeeper-3.4.6.tar.gz "
					+ "&& tar xf zookeeper-3.4.6.tar.gz; fi; "
					+ "if [ \"x`md5sum apache-storm-%s.tar.gz | cut -d ' ' -f 1`\" != \"x%s\" ]; "
					+ "then wget http://mirror.dkd.de/apache/storm/apache-storm-%s/apache-storm-%s.tar.gz "
					+ "&& tar xf apache-storm-%s.tar.gz; fi", this.stormVersionString, stormTarballChecksum,
				this.stormVersionString, this.stormVersionString, this.stormVersionString);
		}
		runVagrantCommand(vagrantDir, new String[] {"vagrant", "ssh", "-c", command});
	}
	
}
