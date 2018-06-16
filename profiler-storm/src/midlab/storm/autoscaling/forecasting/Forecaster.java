package midlab.storm.autoscaling.forecasting;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.Writer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.log4j.Logger;
import org.encog.ml.data.MLData;
import org.encog.ml.data.basic.BasicMLData;
import org.encog.neural.networks.BasicNetwork;
import org.encog.persist.EncogDirectoryPersistence;
import org.jgrapht.graph.DefaultWeightedEdge;
import org.jgrapht.graph.ListenableDirectedWeightedGraph;

import midlab.storm.autoscaling.database.DatabaseConnection;
import midlab.storm.autoscaling.topology.Component;
import midlab.storm.autoscaling.topology.Spout;
import midlab.storm.autoscaling.topology.Topology;
import midlab.storm.autoscaling.utility.Utils;
import midlab.storm.scheduler.data.Allocation;
import midlab.storm.scheduler.data.Executor;
import midlab.storm.scheduler.data.Node;
import midlab.storm.scheduler.data.StormCluster;
import midlab.storm.scheduler.data.Worker;

public class Forecaster {

	private static final Logger logger = Logger.getLogger(Forecaster.class);
	private static final DecimalFormat FORMAT = new DecimalFormat("#.##");
	
	private static final String PROP_FILENAME = "forecaster.properties";
	
	// TODO fix it!!!
	private static int TIMESERIES_DIMENSION = 10;   //for new ANN is 2, for old ANN is 10
	private static int NEURAL_DATE_VARIABLES = 1;   // 1 for syntetic dataset, 2 for fakets dataset, or 5 for complete date
	private static int NEURAL_TIMESERIES = 0;	    // 0, 1, 2, 5, 10
	
	private String dataDirectory;         // directory on which putting forecaster files
	private Topology t;                   // profiled topology
	private BasicNetwork spoutNetwork;    // Neural Netwotk to forecast traffic outgoing from spout
	
	private double[] lastTraffic;         // array containing the last traffic in reverse order (t, t-1, .. t-TIME_SERIES_DIMENSION)	
	private Hashtable<String,Integer>[] lastWorkerCpuUsage;     // !!! for reactive version !!! table containing for each past temporal instant, the worker cpu usage
	private boolean[] overload;                                 // if all the workers are over scale-out threshold
	private boolean[] underload;                                // if all the workers are under scale-in threshold
	
	private Hashtable<Component,Double> expectedLoadTable; // table containing the expected load NORMALIZED for each component
	private Hashtable<Component,Integer> actualComponentParallelism;
	private Hashtable<Component,Integer> maxComponentParallelism;
	
	private long sts;            // how many seconds go ahead from the beginning of the dataset
	private int trf;             // tuple replication factor; is used just for assign the first lastTrafficArray
	private long offset;         // difference between the current time (when a test is running) and the dataset time
	
	private StormCluster stormCluster;  // storm cluter with slotIds, ports and hostnames
	private int clusterSize;            // number of machine available in the cluster
	private List<String> currentConfig; // List of current active workers
	private int currentConfigSize;      // number of current workers
	private int nextConfigSize;         // used only for reactive version;
	                                    // the proactive one compute the next config throgh isMinimumAllocation
	
	@SuppressWarnings("unused")
	private int corePerWorker;
	private long coreSpeed;
	
	private double scaleOutWorkerThreshold;    // threshold of %CPU maximum of each node to scale
	private double scaleInWorkerThreshold;     // threshold of %CPU maximum of each node to scale
	
	private double scaleOutExecutorThreshold;    // threshold of %CPU maximum of each node to scale
	private double scaleInExecutorThreshold;     // threshold of %CPU maximum of each node to scale
	
	private double forecastedTraffic;       // forecasted traffic value normalized from spout (tuple/min)
	private boolean alreadyForecasted;      // says if in the current iteration the traffic has been yet forecasted.
	                                        // If yes, read the traffic from forecastedTraffic variable, otherwise do a forecasting
	
	private int contFileTrafficReadFromDB;  // counter of iterations read from db 
	private int contFileTrafficForecasted;  // counter of iteration of forecasting (to sync with above counter)
	
	private File fileTrafficReadFromDB;     // file on which printing <counter,timestamp,trafficRead>
	private File fileTrafficForecasted;     // file on which printing <counter,timestamp,trafficForecasted>
	private File fileCpuExecutorWorker;
	
	private boolean isCompressedDataset;    // if true use a compressed dataset
	private long startDataset;              // timestamp of start dataset
	
	private boolean isProactive;            // if true is the proactive AutoScaler, otherwise the reactive one
	private boolean debug;
		
	/* TODO list:
	 *   1) check how to set the value of ACKER_MAX_TRAFFIC:
	 *   	maximum tuple rate out from spout + tuple rate out from each component with input their maximum
	 *   2) set properly MAGIC_NUMBER
	 */
	private static final double ACKER_MAX_TRAFFIC = 180622D;	
	private static final double MAGIC_NUMBER = 1.2D;
	
	
		
	/**
	 * Constructor called by AutoScaler
	 * @param dataDirectory directory on which is stored forecaster files (usually called "forecaster")
	 * @param sts 
	 * @param trf
	 * @param proactive
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	public Forecaster(String dataDirectory, int sts, int trf, boolean proactive) throws Exception{
		
		// check the correctness of DATA_VARIABLES
		if (NEURAL_DATE_VARIABLES != 0
			&& NEURAL_DATE_VARIABLES != 1
			&& NEURAL_DATE_VARIABLES != 2
			&& NEURAL_DATE_VARIABLES != 5)
			throw new Exception("NEURAL_DATE_VARIABLES must be set to 0, 1, 2 or 5");
		
		this.dataDirectory = dataDirectory;
		
		// reading properties file
		File dataDirFile = new File(dataDirectory);
		File propFile = new File(dataDirFile, PROP_FILENAME);
		Properties prop = new Properties();
		prop.load(new FileReader(propFile));				
		
		// loading the neural network for forecasting tuple rate
		String neuralName = prop.getProperty("neural_name");
		final File networkFile = new File(dataDirFile, neuralName);
		this.spoutNetwork = (BasicNetwork) EncogDirectoryPersistence.loadObject(networkFile);
			
		// deserializing and loading Topology serialized
		TopologyDeserializer deserializer = new TopologyDeserializer();
		deserializer.deserialzeTopology(new File(dataDirFile,"topology.ser").getAbsolutePath());
		this.t = deserializer.getTopology();	
		
		// creating the lastTraffic array
		this.lastTraffic = new double[TIMESERIES_DIMENSION];
				
		// creating the lastTraffic array and over/under load arrays
		this.lastWorkerCpuUsage = new Hashtable[TIMESERIES_DIMENSION];
		this.overload = new boolean[TIMESERIES_DIMENSION];
		this.underload = new boolean[TIMESERIES_DIMENSION];
		
		// creating the expectedLoadTable table
		this.expectedLoadTable = new Hashtable<Component,Double>();
		
		// setting sts (shift timestamp), trf (tweet replication factor)
		this.sts = sts;
		this.trf = trf;
		this.isCompressedDataset = Boolean.valueOf(prop.getProperty("compressed_dataset"));
		this.startDataset = Long.parseLong(prop.getProperty("start_dataset"));
		
		// setting threshold to scale-out/scale-in
		this.scaleOutWorkerThreshold = Double.parseDouble(prop.getProperty("scale-out_worker_threshold"));
		this.scaleInWorkerThreshold = Double.parseDouble(prop.getProperty("scale-in_worker_threshold"));
		
		this.scaleOutExecutorThreshold = Double.parseDouble(prop.getProperty("scale-out_executor_threshold"));
		this.scaleInExecutorThreshold = Double.parseDouble(prop.getProperty("scale-in_executor_threshold"));
		
		// setting the cluster dimension
		this.clusterSize = Integer.parseInt(prop.getProperty("cluster_size"));
				
		// setting the current config as the cluster dimension
		this.currentConfigSize = this.clusterSize;
		
		// save core_speed and core_per_worker from db
		saveCoreInfo();
		
		// setting debugging or non debugging mode
		this.debug = Boolean.valueOf(prop.getProperty("debug"));;
		
		// setting proactivity
		this.isProactive = proactive;
		
		// assign last traffic
		assignStaticLastArray(prop.getProperty("last_traffic").split(","));
				
		// assign last worker cpu usage values to 0 and under/overload
		assignStaticLastCpuWorkerUsage();
		
		// setting the simulated date with offset = currentTime - startDataset + sts
		this.offset = System.currentTimeMillis() - (startDataset + this.sts*1000);
		Date realDate = new Date(System.currentTimeMillis());
		Date simulatedDate = new Date(getSyncedTimestamp());
				
		// update parallelism of components
		actualComponentParallelism = new Hashtable<Component, Integer>();
		maxComponentParallelism = new Hashtable<Component, Integer>();
		int i = 0;
		String[] componentParallelism = new String[2];
		String nameComponent = "";
		int parallelism = 1;
		while(prop.containsKey("runtime_spout"+i)){
			componentParallelism = prop.getProperty("runtime_spout"+i).split(",");
			nameComponent = componentParallelism[0];
			parallelism = Integer.parseInt(componentParallelism[1]);
			t.getComponent(nameComponent).setParallelism(parallelism);
			maxComponentParallelism.put(t.getComponent(nameComponent), parallelism);
			actualComponentParallelism.put(t.getComponent(nameComponent), parallelism);
			logger.info("\tupdated parallelism of component " + nameComponent + " to " + t.getComponent(nameComponent).getParallelism());
			++i;
		}
		i = 0;
		while(prop.containsKey("runtime_bolt"+i)){
			componentParallelism = prop.getProperty("runtime_bolt"+i).split(",");
			nameComponent = componentParallelism[0];
			parallelism = Integer.parseInt(componentParallelism[1]);
			t.getComponent(nameComponent).setParallelism(parallelism);
			maxComponentParallelism.put(t.getComponent(nameComponent), parallelism);
			actualComponentParallelism.put(t.getComponent(nameComponent), parallelism);
			logger.info("\tupdated parallelism of component " + nameComponent + " to " + parallelism);
			++i;
		}
		
		// creating file for writing traffic read from db and forecasted value
		fileTrafficReadFromDB = new File(dataDirFile,"trafficFromDB.csv");
		fileTrafficForecasted = new File(dataDirFile,"trafficForecasted.csv");
		fileCpuExecutorWorker = new File(dataDirFile,"cpuExecutorWorker.csv");
		fileTrafficReadFromDB.delete();
		fileTrafficForecasted.delete();
		fileCpuExecutorWorker.delete();
		contFileTrafficReadFromDB = 0;
		contFileTrafficForecasted = 1;
		
		// writing the first line for the fileTrafficForecasted (just for aligning the files)
		Writer out = new BufferedWriter(new FileWriter(fileTrafficForecasted, true));
		out.write("0,0,0\n");
		out.flush();
		out.close();
				
		logger.info("Forecaster successfully created. Temporal offset="+offset+"; current date: "+realDate+"; simulated date: "+simulatedDate);
	}
	
	
	
	/**
	 * Return the topology
	 * @return
	 */
	public Topology getTopology(){
		return this.t;
	}
	
	/**
	 * Return the lastTrafficArray
	 * @return
	 */
	public double[] getLastTrafficArray(){
		return this.lastTraffic;
	}
	
	/**
	 * Return the Spout Neural Network
	 * @return
	 */
	public BasicNetwork getSpoutNeuralNetwork(){
		return this.spoutNetwork;
	}
	
	/**
	 * Return the expectedLoadTable
	 * @return
	 */
	public Hashtable<Component,Double> getExpectedLoadTable(){
		return this.expectedLoadTable;
	}
	
		
	/**
	 * Return a map containing the real parallelism of each component
	 * @return
	 */
	public Hashtable<Component,Integer> getActualParallelism() {
		return this.actualComponentParallelism;
	}
	
		
	/**
	 * Verify whether an allocation is minimum to sustain a forecasted traffic
	 * @param sc
	 * @param alloc
	 * @return
	 */
	public boolean isMinimumAllocation(StormCluster sc, Allocation alloc){
		
		Collection<Node> nodes = sc.getNodes();
		
		/** PROACTIVE VERSION **/
		if (isProactive && !debug) {
			Map<Worker,List<Executor>> map = alloc.getAllocation();
			Map<Node, Long> loadMap = new HashMap<Node, Long>();
			logger.info("+++++++++++++++++++++++++++++++++++++++++++");
			logger.info("+++ Checking configuration with "+nodes.size()+" nodes +++");
			logger.info("+++++++++++++++++++++++++++++++++++++++++++");
			logger.info("Nodes selected: "+nodes.toString());
		
			// for each node in the cluster
			for (Node n : nodes) {
				logger.info("Computing expected CPU load for node: "+n);
				Collection<Worker> slot = n.getWorkers();
			
				// for each worker (it should be 1 for each machine)
				for (Worker w : slot) {
					List<Executor> listExecutor = map.get(w);
				
					if (listExecutor != null) {
						// get all the executors in the worker w of node n
						for (Executor exec : listExecutor) {
							
							// get the component related to the current executor
							String componentId = exec.getKey().getComponentId();
							Component component = t.getComponent(componentId);
							
							// get the NORMALIZED expected load (computed previously in computeExpectedLoadTable)
							double expectedLoadNormalized = expectedLoadTable.get(component);
							
							// denormalize the expected load
							double expectedLoad = expectedLoadNormalized * (double)t.getMaxCpuLoad();
							logger.info("\texecutor " + exec.toString()+" load: " + expectedLoad);
							
							// update the current load for the node n by adding the expected load of the current exec
							Long currentLoad = loadMap.get(n);
							if (currentLoad==null)
								currentLoad = new Long(0);
							
							currentLoad += (long)expectedLoad;
							loadMap.remove(n);
							loadMap.put(n, currentLoad);
						}
					}
				}
			
				// now for node n we can estimate the CPU usage
				double expectedCpuUsageFrequency = (double)loadMap.get(n);
				double expectedCpuUsageSingleNode = expectedCpuUsageFrequency / ( (double)t.getMaxCpuLoad()/(double)clusterSize )*100D;
				
				logger.info("Expected CPU load frequency  of "+n+": "+expectedCpuUsageFrequency+" Hz");
				logger.info("Expected CPU load percentage of "+n+": "+FORMAT.format(expectedCpuUsageSingleNode)+"%");				
				
				/* condition to verify for each node: CPU load of node n < threshold.
				 * if just one node don't verify this condition, the tried configuration is not minimal
				 */
				if (loadMap.get(n) > (scaleOutWorkerThreshold * t.getMaxCpuLoad() / (double)clusterSize)) {
					logger.info("The expected load overcome the threshold of "+scaleOutWorkerThreshold*100+"% CPU of node "+n);
					logger.info("---------------------------------------------------------");
					logger.info("--- NO! The configuration with " + nodes.size() + " nodes is not minimal ---");
					logger.info("---------------------------------------------------------\n");
					return false;
				}
				else {
					logger.info("Ok! The node " + n + " can ensure its expected load!");
				}
			}
		
			// if all the node do not overcome the threshold of CPU return true
			logger.info("-----------------------------------------------------");
			logger.info("--- OK! The configuration with " + nodes.size() + " nodes is minimal ---");
			logger.info("-----------------------------------------------------\n");
			currentConfigSize = nodes.size();
			return true;
		}
		
		else if (isProactive && debug) {
			return true; // by returning true, it will always return the config with 1 node; for debug purpose only!!!
		}
		

		/** REACTIVE VERSION **/
		else {
			
			if (nextConfigSize == nodes.size()) {
				logger.info("----------------------------------------------");
				logger.info("--- The best configuration is with "+nodes.size()+" nodes ---");
				logger.info("----------------------------------------------\n");
				currentConfigSize = nextConfigSize;
				return true;
			}
			else
				return false;
		}
		
	}
	
	
	/**
	 * Set the current traffic read from the db in the first entry of the lastTrafficArray, calling shiftCurrentTrafficArray() to shift the old values
	 * @throws Exception
	 */
	public void setCurrentTraffic() throws Exception{
		this.lastTraffic = shiftCurrentTrafficArray();
	}
	
	
	/**
	 * Set the current worker cpu usage read from the db in the first entry of the lastTrafficArray, calling shiftCurrentTrafficArray() to shift the old values
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	public void setCurrentWorkerCpuUsage() throws Exception{
		this.lastWorkerCpuUsage = shiftCurrentWorkerCpuUsageArray();
		writeExecutorTotalCpuAndWorkerCpu();
	}
	
	
	/**
	 * Set which workers are used in the current configuration and the config size
	 * @param workerSlotsUsedString
	 */
	public void setCurrentConfig(List<String> workerSlotsUsedString) {
		this.currentConfig = workerSlotsUsedString;
		this.currentConfigSize = workerSlotsUsedString.size();
	}
	
	
	/**
	 * Set the Storm Cluster
	 * @param stormCluster
	 * @throws Exception 
	 */
	public void setStormCluster(StormCluster stormCluster) {
		this.stormCluster = stormCluster;
	}
	
	
	/**
	 * Call the methods for computing the forecastedTrafficGraph and the loadTable in the proactive version;
	 * do nothing in the reactive version
	 * @throws Exception
	 */
	public void start(Map<String, Integer> componentToParallelismMap) throws Exception{
			
		logger.info("*********************");
		if (debug)
			logger.info("!!!DEBUGGING MODE!!!");
		
		logger.info("Current configuration:");
		for (String id : currentConfig) {
			logger.info("\t- " + id + " -> " + stormCluster.getNodeById(id).getHostname());
		}
		
		// reading parallelism of each component from the AutoScaler
		for (String componentName : componentToParallelismMap.keySet()) {
			if (actualComponentParallelism.containsKey(t.getComponent(componentName))) {
				actualComponentParallelism.put(t.getComponent(componentName), componentToParallelismMap.get(componentName));
				logger.info("Reading actual parallelism of component " + componentName + " from AutoScaler: " + componentToParallelismMap.get(componentName));
			}
			else {
				logger.error("Cannot find any component called " + componentName);
			}
		}
		
		/* check if new parallelism levels are different from the ones set in the topology;
		 * if not, set the correct one read from the AutoScaler
		 */
		for (Component c : t.getComponents()) {
			if (componentToParallelismMap.containsKey(c.getName())) {
				if (c.getParallelism() != componentToParallelismMap.get(c.getName())) {
					logger.warn("Component " + c.getName() + " in the Forecaster has a different parallelism level (" + c.getParallelism() +") than the actual (" + componentToParallelismMap.get(c.getName()) +"). Setting proper level..");
					c.setParallelism(componentToParallelismMap.get(c.getName()));
				}
			}
		}
		
		/** PROACTIVE VERSION **/
		if (this.isProactive) {			
			logger.info("Start forecasting phase...\n");
			this.alreadyForecasted = false;
			computeForecastedTrafficGraph();
			
			/* checking the right parallelism level for each component;
			 * iterate till all components have a proper parallelism level
			 */
			logger.info("Looking for overloaded operators...");
			int iter = 1;
			ComponentToUpdate ctu = computeExpectedLoadTable(iter);
			Set<Component> overLoadedComponents = ctu.getOverLoadedComponents();
			Set<Component> underLoadedComponents = ctu.getUnderLoadedComponents();
			
			while (ctu.toUpdate()) {
				
				// component to scale up
				logger.info("Following components need to be scaled up");
				int componentNumber = 0;
				for (Component c : overLoadedComponents) {
					componentNumber++;
					int parallelism = c.getParallelism();
					parallelism++;
					c.setParallelism(parallelism);
					if (componentNumber == overLoadedComponents.size())
						logger.info("\t- " + c.getName() + " from " + (parallelism-1) + " to " + parallelism + "\n");
					else
						logger.info("\t- " + c.getName() + " from " + (parallelism-1) + " to " + parallelism);
				}
				
				// component to scale down
				logger.info("Following components need to be scaled down");
				componentNumber = 0;
				for (Component c : underLoadedComponents) {
					componentNumber++;
					int parallelism = c.getParallelism();
					parallelism--;
					c.setParallelism(parallelism);
					if (componentNumber == underLoadedComponents.size())
						logger.info("\t- " + c.getName() + " from " + (parallelism+1) + " to " + parallelism + "\n");
					else
						logger.info("\t- " + c.getName() + " from " + (parallelism+1) + " to " + parallelism);
				}
				
				logger.info("Cheking if they need to be scaled up/down more...");
				++iter;
				ctu = computeExpectedLoadTable(iter);
				
				if (iter > 1 && debug)
					break;
			}
		}
		
		
		/** REACTIVE VERSION **/
		else {
			logger.info("Start reacting to worker cpu usage..\n");
			checkBestReactiveConfig();
		}
	}
	
	
	/**
	 * Assigns statically the value of load of last n minutes
	 * @param last the String[] representation of "last_traffic" parameter into properties file
	 */
	private void assignStaticLastArray(String[] last){
		
		/* 
		 * These values are the total amount of traffic in a minute per seconds and
		 * are statically assigned at the construction of this object.
		 * 
		 * for compressedDataset=false with
		 * 		- sts = 1428600 (that correspond to row 23810 of dataset out-romasicura-count-per-min)
		 * 		- TIMESERIES_DIMENSION = 2
		 * 
		 * for compressedDataset=false with:
		 * 		- sts = 960000 (that correspond to row 16000 of dataset out-romasicura-count-per-min)
		 * 		- TIMESERIES_DIMENSION = 10
		 * 		- double[] staticArray = {83.0, 77.0, 75.0, 85.0, 77.0, 86.0, 85.0, 96.0, 81.0, 93.0};
		 * 
		 * for compressedDataset=true with:
		 * 		- sts = 19920
		 * 		- TIMESERIES_DIMENSION = 10
		 * 		- double[] staticArray = {274.0, 389.0, 496.0, 940.0, 1443.0, 2087.0, 2490.0, 3741.0, 3731.0, 3785.0};
		 */
		
		//double[] staticArray = {274.0, 389.0, 496.0, 940.0, 1443.0, 2087.0, 2490.0, 3741.0, 3731.0, 3785.0};
		
		double[] staticArray = new double[TIMESERIES_DIMENSION];
		for (int i=0; i<TIMESERIES_DIMENSION; ++i) {
			staticArray[i] = Double.parseDouble(last[i]);
		}
		
		// transform the values from event/min in event/sec * trf
		for (int i=0; i<staticArray.length; ++i) {
			staticArray[i] = (staticArray[i]/60.0)*(double)trf;
		}
		
		// copy the staticArray in the lastTraffic array by selecting just the first TIMESERIES_DIMENSION values
		logger.info("Assigned static last traffic values:");
		for (int i=0; i<lastTraffic.length; ++i) {
			lastTraffic[i] = staticArray[i];
			logger.info("\t t-" + (i+1) + " = " + lastTraffic[i]);
		}
	}
	
	
	/**
	 * Assigns statically the value of load of last n minutes
	 * @throws Exception
	 */
	private void assignStaticLastCpuWorkerUsage() throws Exception{
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		
		Hashtable<String, Integer> hostnamesLoad = new Hashtable<String, Integer>();
						
		String query = "select distinct HOSTNAME from APP.WORKER";
		conn = DatabaseConnection.connect(dataDirectory);
		ps = conn.prepareStatement(query);
		rs = ps.executeQuery();
		
		while (rs.next()){
			hostnamesLoad.put(rs.getString(1), 1);
		}
		
		for (int i=0; i<TIMESERIES_DIMENSION; ++i) {
			this.lastWorkerCpuUsage[i] = hostnamesLoad;
			this.overload[i] = false;
			this.underload[i] = true;
		}
		
		if (conn!=null)
			conn.close();
		if (ps!=null)
			ps.close();
		if (rs!=null)
			rs.close();
	}
	
	
	/**
	 * Check which is the best configuration in the reactive version
	 */
	private void checkBestReactiveConfig() {
		logger.info("checking overload or underload...");
		for (int i=0; i<lastWorkerCpuUsage.length; ++i) {
			
			// check if the cluster at time 't-i' is overloaded
			overload[i] = false;
			for (String hostname : lastWorkerCpuUsage[i].keySet()) {
				logger.debug("checking " + hostname + " usage: " + lastWorkerCpuUsage[i].get(hostname));
				if (lastWorkerCpuUsage[i].get(hostname) > scaleOutWorkerThreshold*100) {
					logger.info("overcome the scale-out threshold of " + scaleOutWorkerThreshold*100 + "% !!");
					overload[i] = true;
					break; // for policy 'at least one worker must be overload'
				}
				else {
					logger.debug("doesn't overcome the scale-out threshold of " + scaleOutWorkerThreshold*100 + "%. Break the cycle");
					overload[i] = false;
					//break; // for policy 'all workers must be overload'
				}
			}
			
			// check if the cluster at time 't-i' is underloaded
			underload[i] = false;
			for (String hostname : lastWorkerCpuUsage[i].keySet()) {
				logger.debug("checking " + hostname + " usage: " + lastWorkerCpuUsage[i].get(hostname));
				if (lastWorkerCpuUsage[i].get(hostname) < scaleInWorkerThreshold*100) {
					logger.debug("less than the scale-in threshold of " + scaleInWorkerThreshold*100 + "% !!");
					underload[i] = true;
					//break; //for policy 'at least one worker must be underload'
				}	
				else {
					logger.debug("is not less than the scale-in threshold of " + scaleInWorkerThreshold*100 + "%. Break the cycle");
					underload[i] = false;
					break; // for policy 'all workers must be underload'
				}
			}	
			
		}
		
		// print over/underloading arrays
		for (int i=0; i<overload.length; ++i) {
			logger.info("overload[t-" + i + "] = " + overload[i]);
		}
		for (int i=0; i<underload.length; ++i) {
			logger.info("underload[t-" + i + "] = " + underload[i]);
		}
		
		int consideredPortionWindow = 2;
		logger.info("Considering 1/"+consideredPortionWindow+" as window..");
		
		// check if in all the timeseries the system was overloaded
		boolean isOverload = true;
		for (int i=0; i<lastWorkerCpuUsage.length/consideredPortionWindow; ++i) {
			// policy1: 'in all time window workers must be overload'
			/*
			if (!overload[i]) {
				isOverload = false;
				break;
			}
			*/
			
			//policy2: 'in at least on time window workers must be overload'
			if (overload[i]) {
				isOverload = true;
				break;
			}
			else {
				isOverload = false;
			}
		}
		
		// check if in all the timeseries the system was overloaded
		boolean isUnderload = true;
		for (int i=0; i<lastWorkerCpuUsage.length/consideredPortionWindow; ++i) {
			//policy1: 'in all time window workers must be underload'
			if (!underload[i]) {
				isUnderload = false;
				break;
			}
			
			// policy2: 'in at least on time window workers must be underload'
			/*if (underload[i]) {
				isUnderload = true;
				break;
			}
			else {
				isUnderload = false;
			}
			*/
		}
		
		if (isOverload && isUnderload)
			logger.error("Something wrong... the system could not be in overload and underloading at the same time!");
		
		else {
			if (!isOverload && !isUnderload)
				this.nextConfigSize = currentConfigSize;
			if (isOverload && currentConfigSize < clusterSize)
				nextConfigSize = currentConfigSize + 1;
			if (isOverload && currentConfigSize == clusterSize)
				nextConfigSize = currentConfigSize;
			if (isUnderload && currentConfigSize > 1)
				nextConfigSize = currentConfigSize - 1;
			if (isUnderload && currentConfigSize == 1)
				nextConfigSize = currentConfigSize;
		}
	}
	
	
	/**
	 * Update the graph of topology with the weights of each edge equals to the traffic forecasted.
	 * The values on the edge is NOT NORMALIZED
	 * @throws Exception 
	 */
	private void computeForecastedTrafficGraph() throws Exception{
		logger.info("++++++++++++++++++++++++++++++++++++++++++");
		logger.info("+++ Computing forecasted traffic graph +++");
		logger.info("++++++++++++++++++++++++++++++++++++++++++");
		
		//updating traffic in each node
		Set<Component> sinkNodes = t.getSinkNodes();
		Iterator<Component> itSinkNode = sinkNodes.iterator();
		while (itSinkNode.hasNext()) {
			Component currentComponent = itSinkNode.next();
			logger.info("Founded sink node "+currentComponent+". Starting recursive graph visit..");
			computeTrafficIn(currentComponent);
		}
		logger.info("------------------------------");
		logger.info("--- Traffic graph computed ---");
		logger.info("------------------------------\n");
	}
	
	
	/**
	 * On the basis of the updated graph with the forecasted traffic, compute the expected load table.
	 * The values in the table ARE NORMALIZED.
	 * NOTE: THE TRAFFIC INTO THE TOPOLOGY GRAPH IS UPDATED WITH THE METHOD computeTrafficIn()
	 */
	private ComponentToUpdate computeExpectedLoadTable(int iter){
		logger.info("++++++++++++++++++++++++++++++++++++++++++++++++++");
		logger.info("+++ Computing expected load table: iteration " + iter + " +++");
		logger.info("++++++++++++++++++++++++++++++++++++++++++++++++++");
		
		ComponentToUpdate ctu = new ComponentToUpdate();
		Set<Component> overLoadedComponents = new HashSet<Component>();
		Set<Component> underLoadedComponents = new HashSet<Component>();
		
		for(Component currentComponent : t.getGraph().vertexSet()) {
			
			// getting the current component neural network for inferencing load from traffic
			BasicNetwork network = t.getLoadTable().get(currentComponent);
			
			// saving the current traffic (tuple/sec) flowing in the current component (NOT NORMALIZED!) or out traffic from spout
			String inputOrOutput = "";
			String boltOrSpout = "";
			double[] inputTraffic = new double[1];
			if(currentComponent.isBolt()){
				inputTraffic[0] = t.getTrafficIn(currentComponent);
				inputOrOutput = "input";
				boltOrSpout = "bolt";
			}
			if(currentComponent.isSpout()){
				// TODO check this line: here I multiply per 2 due to the traffic towards the _acker
				inputTraffic[0] = 2* ((this.forecastedTraffic * t.getMaxTrafficPerMinute()) / 60D);
				inputOrOutput = "output";
				boltOrSpout = "spout";
				logger.info("!!! Notice that the output traffic of a spout is double (half to topology, half to _acker)");
			}
			double inputTrafficComponent = inputTraffic[0];
			double inputTrafficExecutors = inputTrafficComponent / (double)currentComponent.getParallelism();
			logger.info("Estimatied load of "+currentComponent+ " ("+boltOrSpout+", parallelism "+currentComponent.getParallelism()+")");
			logger.info("\t"+inputOrOutput+" traffic of component "+currentComponent.getName()+": "+inputTrafficComponent);
			logger.info("\t"+inputOrOutput+" traffic of "+currentComponent+"'s executor: "+inputTrafficExecutors);
						
			// normalize input traffic; note that also the maxTrafficIn is divided by the component parallelism
			if(currentComponent.isBolt()) {
				if (currentComponent.getName().equals("__acker"))
					inputTraffic[0] = inputTrafficExecutors  / ACKER_MAX_TRAFFIC;
				else
					inputTraffic[0] = inputTrafficExecutors  / ((double)t.getMaxTrafficIn(currentComponent) );
			}
			//inputTraffic[0] = inputTrafficExecutors  / ((double)t.getMaxTrafficIn(currentComponent) /* /(double)currentComponent.getParallelism()*/);
			if(currentComponent.isSpout()) inputTraffic[0] = inputTrafficExecutors  / (t.getMaxTrafficPerSecond());
			
			// inserting the input in ML object
			BasicMLData neuralInput = new BasicMLData(inputTraffic);
			
			// calculating the output -> inferencing load from traffic 
			MLData neuralOutput = network.compute(neuralInput);
			double[] output = neuralOutput.getData();
			for (int i=0; i<output.length; ++i) {
				if (output[i] < 0)
					output[i] = 0;
				
				/*TODO: qui moltiplicavo per 60 perchè non tornava il carico dell'acker:
				 * RISOLTO con la nuova normalizzazione?
				 * if (currentComponent.isBolt() && currentComponent.getName().equals("__acker"))
				 *     output[i] *= 60;
				*/	
			}
				
			double expectedExecutorLoad = output[0] * t.getMaxCpuLoad();
			
			if (debug) {
				if ((currentComponent.getName().equals("stopWordFilter") || currentComponent.getName().equals("wordGenerator")) && currentComponent.getParallelism() > 0)
					underLoadedComponents.add(currentComponent);
				
				if ((currentComponent.getName().equals("counter") || currentComponent.getName().equals("tweetReader")) && currentComponent.getParallelism() < maxComponentParallelism.get(currentComponent))
					overLoadedComponents.add(currentComponent);
			}
			else {
				if (expectedExecutorLoad < this.scaleInExecutorThreshold * this.coreSpeed && currentComponent.getParallelism() > 0)
					underLoadedComponents.add(currentComponent);
				
				if (expectedExecutorLoad > this.scaleOutExecutorThreshold * this.coreSpeed && currentComponent.getParallelism() < maxComponentParallelism.get(currentComponent))
					overLoadedComponents.add(currentComponent);
			}
				
			ctu.setOverLoadedComponents(overLoadedComponents);
			ctu.setUnderLoadedComponents(underLoadedComponents);
			
			
			// inserting the expected load in the table (NORMALIZED)
			expectedLoadTable.put(currentComponent, output[0]);
			logger.info("\texecutor's " + inputOrOutput + " traffic: " + FORMAT.format(inputTrafficExecutors) + " (normal value " + inputTraffic[0]);
			logger.info("\tforecasted load: " + expectedExecutorLoad + " (normal value to check on encog: "+output[0]+")");
		}
		logger.info("------------------------------------");
		logger.info("--- Expected load table computed ---");
		logger.info("------------------------------------\n");
		
		return ctu;
		
	}
	
		
	/**
	 * Compute the input traffic of a given component (at logical level, not at executor level!) in tuple/sec and NOT NORMALIZED
	 * @param c
	 * @return
	 * @throws Exception 
	 */
	private double computeTrafficIn(Component c) throws Exception{
		ListenableDirectedWeightedGraph<Component,DefaultWeightedEdge> g = t.getGraph();
		double inWeight = 0.0;
		double weight = 0.0;
		double alfa = 0.0;
		Set<DefaultWeightedEdge> inEdges = g.incomingEdgesOf(c);
		Iterator<DefaultWeightedEdge> itInEdge = inEdges.iterator();
		DefaultWeightedEdge currentEdge;
		while(itInEdge.hasNext()){
			currentEdge = itInEdge.next();
			Component cSource = g.getEdgeSource(currentEdge);
			if(cSource.isSpout()){
				/* weight has values in tuple/sec NOT NORMALIZED so we have to:
				 * 		1) compute the forecasted traffic of the component: forecastedTraffic(cSource) return the normalized value of the max traffic (tuple/min)
				 * 		2) denormalize (by multiplying per maxTrafficPerMinute)
				 * 		3) transform in tuple/sec (by dividing per 60)
				 */
				if (this.alreadyForecasted)
					weight = (forecastedTraffic * t.getMaxTrafficPerMinute()) / 60D;
				else
					weight = (forecastTraffic(cSource) * t.getMaxTrafficPerMinute()) / 60D;
				logger.info("Computing traffic inter-component. Forecasted out traffic from spout: "+FORMAT.format(weight)+" tuple/sec");
			}
			else{
				alfa = t.getAlfaTable().get(currentEdge);
				weight = alfa * computeTrafficIn(cSource);
				logger.info("\tedge "+currentEdge.toString()+": alfa="+FORMAT.format(alfa)+" weight="+FORMAT.format(weight)+" (expected tuple/sec)");
			}
			g.setEdgeWeight(currentEdge, weight);
			t.updateGraphInTopology(g);
			inWeight += weight;
		}
		return inWeight;
	}
	
	
	/**
	 * Return the max forecasted traffic in the next temporal horizon NORMALIZED in tuple/min
	 * @param c
	 * @return max traffic in tuple/min (denormalized and without trf)
	 * @throws Exception 
	 */
	private double forecastTraffic(Component c) throws Exception{
		
		// getting the timeseries array by calling shiftArray() method that shifts the array stores on head the current load
		//lastTraffic = shiftArray();
		
		// getting the timestamp and transform in date variables as ANN input
		//TODO: il metodo qui sotto prima moltiplicava il timestamp x1000; ora sembra non servire più: perchè???
		long timestamp = getSyncedTimestamp();
		String normalizedDate = Utils.convertTimestampInNormalizedDate(timestamp);
		String[] normalizedDateArr = normalizedDate.split(",");
		double day = Double.parseDouble(normalizedDateArr[0]);
		double month = Double.parseDouble(normalizedDateArr[1]);
		double date = Double.parseDouble(normalizedDateArr[2]);
		double hour = Double.parseDouble(normalizedDateArr[3]);
		double minutes = Double.parseDouble(normalizedDateArr[4]);
		logger.info("Forecasting traffic at TS="+System.currentTimeMillis()+" (SHIFTED_TS="+timestamp+", "+new Date(timestamp)+")");
				
		// creates the ANN input with (i) date values and (ii) timeseries values
		logger.info("Printing lastTrafficArray (the first value is the last minute)..");
		for(int j=0; j<lastTraffic.length; ++j){
			logger.info("\tlast_traffic["+j+"] = "+FORMAT.format(lastTraffic[j])+" tuple/sec ("+FORMAT.format(lastTraffic[j]*60.0)+" tuple/min) -> normalized: "+( (lastTraffic[j]*60.0) / (t.getMaxTrafficPerMinute() /*già moltiplicato per trf*/) ) );
		}
		
		double[] input = new double[NEURAL_TIMESERIES + NEURAL_DATE_VARIABLES];
		
		if (NEURAL_DATE_VARIABLES == 5) {
			input[0] = day;
			input[1] = month;
			input[2] = date;
			input[3] = hour;
			input[4] = minutes;
		}
		
		if (NEURAL_DATE_VARIABLES == 2) {
			input[0] = day;
			input[1] = hour;
		}
		
		if (NEURAL_DATE_VARIABLES == 1) {
			input[0] = hour;
		}
		
		for(int i=NEURAL_DATE_VARIABLES; i<input.length; ++i){
			// lastTraffic has req/sec, so I multiply per 60 to have req/min and then I normalize
			//logger.info("inserting "+FORMAT.format(lastTraffic[i-5]*60.0)+" in input "+i+", normalizing over "+t.getMaxTrafficPerMinute()+" and dividing over "+trf);
			input[i] = (lastTraffic[i-NEURAL_DATE_VARIABLES]*60.0) / (t.getMaxTrafficPerMinute());
		}
		logger.info("Neural Network Input:");
		for(int i=0; i<input.length; ++i){
			if (NEURAL_DATE_VARIABLES == 5) {
				// don't need to multiply per trf because contains values read from db
				if(i>=5) logger.info("\tinput "+i+": "+input[i]+" ("+FORMAT.format(input[i]*t.getMaxTrafficPerMinute() )+" tuple/min -> "+FORMAT.format(input[i]*t.getMaxTrafficPerMinute() / 60D )+" tuple/sec)");
				else if(i==0) logger.info("\tinput "+i+": "+input[i]+" day");
				else if(i==1) logger.info("\tinput "+i+": "+input[i]+" month");
				else if(i==2) logger.info("\tinput "+i+": "+input[i]+" date");
				else if(i==3) logger.info("\tinput "+i+": "+input[i]+" hour");
				else if(i==4) logger.info("\tinput "+i+": "+input[i]+" minutes");
				else logger.info("\tinput "+i+": "+input[i]);
			}
			if (NEURAL_DATE_VARIABLES == 2) {
				// don't need to multiply per trf because contains values read from db
				if(i>=2) logger.info("\tinput "+i+": "+input[i]+" ("+FORMAT.format(input[i]*t.getMaxTrafficPerMinute() )+" tuple/min -> "+FORMAT.format(input[i]*t.getMaxTrafficPerMinute() / 60D )+" tuple/sec)");
				else if(i==0) logger.info("\tinput "+i+": "+input[i]+" day");
				else if(i==1) logger.info("\tinput "+i+": "+input[i]+" hour");
				else logger.info("\tinput "+i+": "+input[i]);
			}
			if (NEURAL_DATE_VARIABLES == 1) {
				if(i>=1) logger.info("\tinput "+i+": "+input[i]+" ("+FORMAT.format(input[i]*t.getMaxTrafficPerMinute() )+" tuple/min -> "+FORMAT.format(input[i]*t.getMaxTrafficPerMinute() / 60D )+" tuple/sec)");
				if(i==0) logger.info("\tinput "+i+": "+input[i]+" hour");
			}
		}
		BasicMLData data = new BasicMLData(input);

		// compute the forecast and get the output values
		MLData computation = spoutNetwork.compute(data);
		double[] output = computation.getData();
		logger.info("Computing forecasts for next "+output.length+" minutes...");
		logger.info("Neural Network Output: ");
		for (int i=0; i<output.length; ++i) {
			if(output[i]<0){
				//output[i] contains tuple/min normalized
				output[i] = 0;
				logger.info("\tnext "+(i+1)+" minute: "+FORMAT.format(output[i] * (t.getMaxTrafficPerMinute() /*già moltiplicato per trf*/) )+" tuple/min (converted to 0.0 because it's negative)");
			}
			else{
				output[i] *= MAGIC_NUMBER;
				logger.info("\tnext "+(i+1)+" minute: "+FORMAT.format(output[i] * (t.getMaxTrafficPerMinute() /*già moltiplicato per trf*/) )+" tuple/min ("+FORMAT.format( (output[i]/60D) * (t.getMaxTrafficPerMinute() /*già moltiplicato per trf*/) )+" tuple/sec)");
			}
		}
		
		// find the max traffic value for the next minutes
		double max = -1.0;
		int maxIndex = 0;
		for(int i=0; i<output.length; ++i){
			if(output[i]>max){
				max = output[i];
				maxIndex = i;
			}
		}
		logger.info("Max traffic in the next "+output.length+" minutes is estimated at minute "+(maxIndex+1)+" with "+FORMAT.format(max * t.getMaxTrafficPerMinute() )+" tuple/min ("+FORMAT.format(max/60D * t.getMaxTrafficPerMinute() )+" tuple/sec)");
		
		// writing forecasted traffic on file
		logger.info("Writing computed output on file..");
		double[] traffic = new double[output.length];
		for(int i=0; i<output.length; ++i){
			traffic[i] = output[i]/60D * (t.getMaxTrafficPerMinute() /*già moltiplicato per trf*/) ;
		}
		writeFileTraffic(traffic, false);
		
		// set the forecastedTraffic variable
		this.forecastedTraffic = max;
		
		// set the alreadyForecasted flag to true
		this.alreadyForecasted = true;
		
		return max;
	}
	
	
	/**
	 * Simulate an old ts by subtracting System.currentTimeMillis() - offset
	 * @return
	 */
	private long getSyncedTimestamp(){
		if (isCompressedDataset) {
			long end = System.currentTimeMillis() - offset;
			return (this.startDataset + (( end - this.startDataset) * 60));
		}
		
		else
			return System.currentTimeMillis() - offset;
	}
	
	/**
	 * Store the core info for executor threshold
	 * @throws Exception
	 */
	private void saveCoreInfo() throws Exception {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		
		String query = "select CORE_COUNT,CORE_SPEED from APP.NODE";
		conn = DatabaseConnection.connect(dataDirectory);
		ps = conn.prepareStatement(query);
		rs = ps.executeQuery();
		while (rs.next()) {
			this.corePerWorker = rs.getInt(1);
			this.coreSpeed = rs.getLong(2);
			break;
		}
		
		if (conn!=null)
			conn.close();
		if (ps!=null)
			ps.close();
		if (rs!=null)
			rs.close();
	}
	
	
	/**
	 * Read from DB the traffic outgoing from spouts in tuple/sec in the last minute with a sliding window of 10sec
	 * @return the traffic read in tuple/sec
	 * @throws Exception
	 */
	private int saveCurrentTraffic() throws Exception{
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		String task = "";
				
		Set<Spout> spouts = t.getSpouts();
		Iterator<Spout> itSpout = spouts.iterator();
		while(itSpout.hasNext()){
			Spout currentSpout = itSpout.next();
			ArrayList<Integer> tasksId = currentSpout.getTasks();
			Iterator<Integer> itTaskId = tasksId.iterator();
			task = itTaskId.next().toString();
			while(itTaskId.hasNext()){
				task += " OR SOURCE_TASK_ID="+itTaskId.next().toString()+"";
			}
		}		
		
		int totTraffic = 0;
		String query = "select TRAFFIC from APP.TRAFFIC where SOURCE_TASK_ID="+task;
		conn = DatabaseConnection.connect(dataDirectory);
		ps = conn.prepareStatement(query);
		rs = ps.executeQuery();
		while (rs.next())
			totTraffic += Integer.parseInt(rs.getString(1));
		
		logger.info("Reading traffic from DB.. traffic read = " + totTraffic + " tuple/sec in last minute");
		totTraffic /= 2;
		logger.info("Storing traffic not considering __acker = " + totTraffic);
		
		if (conn!=null)
			conn.close();
		if (ps!=null)
			ps.close();
		if (rs!=null)
			rs.close();
		
		//writing traffic read on file
		double[] traffic = {totTraffic};
		writeFileTraffic(traffic, true);
		
		return totTraffic;
	}
	
	
	/**
	 * Save the current cpu usage of the worker in the current configuration
	 * @return an Hashtable with the hostname and the related cpu usage percentage
	 * @throws Exception
	 */
	/* TODO not yet implemented for multiple workers per node (multiple ports);
	 * currenlty I only read the newest timestamp
	 */
	private Hashtable<String,Integer> saveCurrentWorkerCpuUsage() throws Exception{
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		
		String hostname = "";
		@SuppressWarnings("unused")
		int port = 0;
		String ts = "";
		int cpu_usage = 0;
		
		Hashtable<String,Integer> workerCpuUsage = new Hashtable<String, Integer>();
		Hashtable<String, Date> hostnameTs = new Hashtable<String, Date>();
		
		String hostnames = "";
		for (String id : this.currentConfig) {
			logger.debug("id of worker in current config = " + id);
			hostname = stormCluster.getNodeById(id).getHostname();
			logger.debug("hostname of worker " + id +" = " + hostname);
			if (hostnames.equals(""))
				hostnames += " where HOSTNAME='" + hostname + "'";
			else
				hostnames += " OR HOSTNAME='" + hostname + "'";
		}
		
		String query = "select * from APP.WORKER"+hostnames;
		logger.info(query);
		conn = DatabaseConnection.connect(dataDirectory);
		ps = conn.prepareStatement(query);
		rs = ps.executeQuery();
		
		while (rs.next()){
			hostname = rs.getString("HOSTNAME");
			port = Integer.parseInt(rs.getString("PORT"));
			ts = rs.getString("TS");
			cpu_usage = Integer.parseInt(rs.getString("CPU_USAGE"));
			
			DateFormat format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss", Locale.ENGLISH);
			Date date = format.parse(ts.substring(0, 19));
			if (hostnameTs.containsKey(hostname)) {
				if (hostnameTs.get(hostname).before(date)) {
					hostnameTs.put(hostname, date);
					workerCpuUsage.put(hostname, cpu_usage);
					logger.info("Updating cpu usage for worker " + hostname + " = "+ cpu_usage +"% in last minute");
				}	
			}
			else {
				hostnameTs.put(hostname, date);
				workerCpuUsage.put(hostname, cpu_usage);
				logger.info("Saving cpu usage read from DB.. cpu read for worker " + hostname + " = "+ cpu_usage +"% in last minute");
			}	
		}
			
		if (conn!=null)
			conn.close();
		if (ps!=null)
			ps.close();
		if (rs!=null)
			rs.close();
		
		return workerCpuUsage;
	}
	
	
	/**
	 * Shift currentTrafficArray, so the head is always the last traffic read from the db
	 * @return
	 * @throws Exception
	 */
	private double[] shiftCurrentTrafficArray() throws Exception{
		double[] newArr = new double[lastTraffic.length];
		newArr[0] = saveCurrentTraffic();
		for(int i=1; i<lastTraffic.length; ++i){
			newArr[i] = lastTraffic[i-1];
		}
		return newArr;
	}
	
	
	/**
	 * Shift currentWorkerCpuUsageArray, so the head is always the last cpu value read from the db
	 * @return
	 * @throws Exception
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private Hashtable[] shiftCurrentWorkerCpuUsageArray() throws Exception{
		Hashtable<String,Integer>[] newArr = new Hashtable[lastWorkerCpuUsage.length];
		newArr[0] = saveCurrentWorkerCpuUsage();
		for(int i=1; i<lastWorkerCpuUsage.length; ++i){
			newArr[i] = lastWorkerCpuUsage[i-1];
		}
		return newArr;
	}
	
	
	/**
	 * Write on 2 files; on file forecastedTraffic.csv writes <cont,ts,forecasted_traffic> and on trafficFromDB.csv writes <cont,ts,traffic_read>
	 * @param traffic
	 * @param isReadFromDb
	 * @throws Exception
	 */
	private void writeFileTraffic(double[] traffic, boolean isReadFromDb) throws Exception{
		Writer out;
		if(isReadFromDb){
			out = new BufferedWriter(new FileWriter(fileTrafficReadFromDB, true));
			out.write(contFileTrafficReadFromDB+","+getSyncedTimestamp()+","+traffic[0]+"\n");
			contFileTrafficReadFromDB++;
		}	
		else{
			out = new BufferedWriter(new FileWriter(fileTrafficForecasted, true));
			long timestamp = getSyncedTimestamp();
			for(int i=0; i<traffic.length; ++i){
				timestamp = timestamp + ((i+1)*60000);
				out.write(contFileTrafficForecasted+","+timestamp+","+traffic[i]+"\n");
				contFileTrafficForecasted++;
			}
		}
		out.flush();
		out.close();
	}
	
	
	/**
	 * Write the actual CPU usage for each worker and the cpu used by the executor
	 * @throws Exception 
	 */
	private void writeExecutorTotalCpuAndWorkerCpu() throws Exception {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		
		// prepare the output stream and write the first columns (counter and timestamp)
		Writer out;
		out = new BufferedWriter(new FileWriter(fileCpuExecutorWorker, true));
		out.write(contFileTrafficReadFromDB+","+getSyncedTimestamp());
		
		// get the last instance of worker cpu usage
		Hashtable<String, Integer> workerCpuUsage = lastWorkerCpuUsage[0];	
		
		// prepare the query for each worker hostname
		String querySuffix = "select SUM(LOAD) from APP.LOAD where WORKER_HOSTNAME='";
		String query = "";
		boolean executed = false;
		for (String workerName : workerCpuUsage.keySet()) {
			// get the worker load
			int workerLoad = workerCpuUsage.get(workerName);
			
			// execute the query
			query = "";
			query = querySuffix + workerName + "'";
			logger.info(query);
			try{
			conn = DatabaseConnection.connect(dataDirectory);
			ps = conn.prepareStatement(query);
			rs = ps.executeQuery();
			
			// get the executor load and write the values on file
			long executorLoad = 0L;
			while (rs.next()) {
				executorLoad = rs.getLong(1);
				out.write("," + executorLoad + "," + (double)(executorLoad/(4L*2936012800L)) +"," + workerLoad);
				out.flush();
				executed = true;
			}
			} catch (SQLException e) {
				logger.info("Impossible to execute the query for the worker " + workerName);
				executed = false;
			}
		}
		
		// close streams and connections
		if (executed) {
			out.write("\n");
		out.flush();
		out.close();
		}
		
		if (conn!=null)
			conn.close();
		if (ps!=null)
			ps.close();
		if (rs!=null)
			rs.close();
		
	}
	
	
	
	class ComponentToUpdate {
		private Set<Component> overLoadedComponents;
		private Set<Component> underLoadedComponents;
		
		public void setOverLoadedComponents(Set<Component> overLoadedComponents) {
			this.overLoadedComponents = overLoadedComponents;
		}
		
		public void setUnderLoadedComponents(Set<Component> underLoadedComponents) {
			this.underLoadedComponents = underLoadedComponents;
		}
		
		public Set<Component> getOverLoadedComponents() {
			return this.overLoadedComponents;
		}
		
		public Set<Component> getUnderLoadedComponents() {
			return this.underLoadedComponents;
		}
		
		public boolean toUpdate() {
			if (this.overLoadedComponents.size() > 0 || this.underLoadedComponents.size() > 0)
				return true;
			else
				return false;
		}
	}
}
