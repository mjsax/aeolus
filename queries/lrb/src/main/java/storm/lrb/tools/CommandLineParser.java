package storm.lrb.tools;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.beust.jcommander.Parameter;


/**
 * helper class to ease parsing of arguments
 *
 */
public class CommandLineParser {

	
	@Parameter
	  public List<String> parameters = new ArrayList<String>();
	 
	  @Parameter(names = { "-p", "-port" }, description = "SocketPort to connect to (default: 15000)")
	  public Integer port = 15000;
	  

	  @Parameter(names = { "-x", "-xway" }, description = "How many xways to process")
	  public Integer xways = 1;
	  
	  
	  @Parameter(names = { "-o", "-offset" }, description = "Offset to start simulation with")
	  public Integer offset = 0;
	  
	  @Parameter(names = { "-f", "-file" }, description = "Simulation file to use")
	  public String file = null;
	 
	  @Parameter(names = "-h", description = "SocketHost to connect to (default: localhost)")
	  public String host = "localhost";
	  
	  @Parameter(names = "-submit", description = "Submit to Cluster")
	  public boolean submit = false;
	  
	  @Parameter(names = { "-hist", "-histfile" }, description = "History File to consume")
	  public String histFile = "";
	  
	  @Parameter(names = { "-worker", "-w" }, description = "Number of workers")
	  public int workers = 1;
	  
	  @Parameter(names = { "-executors", "-e" }, description = "Number of executors")
	  public int executors = 1;
	  
	  @Parameter(names = { "-tasks", "-t" }, description = "Number of tasks")
	  public int tasks = 3;
	  
	  @Parameter(names = { "-debug", "-d" }, description = "Set debug mode")
	  public boolean debug = false;
	  
	  @Parameter(names = "-fields", variableArity = true)
	  public List<String> fields = Arrays.asList("xway", "dir");

	  
	  
	  @Parameter(names = { "-n", "-name" }, description = "prefix for topology name")
	public String nameext = "";

	  
}
