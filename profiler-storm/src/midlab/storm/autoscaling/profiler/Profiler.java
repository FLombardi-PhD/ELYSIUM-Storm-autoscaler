package midlab.storm.autoscaling.profiler;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.util.Properties;
import java.util.Set;

import midlab.storm.autoscaling.topology.Component;
import midlab.storm.autoscaling.topology.Topology;
import midlab.storm.autoscaling.topology.TopologyBuilder;
import midlab.storm.autoscaling.utility.GraphUtils;
import midlab.storm.autoscaling.utility.Utils;

import org.jgrapht.graph.DefaultWeightedEdge;
import org.jgrapht.graph.ListenableDirectedWeightedGraph;
import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;

/**
 * Profiler Main Class
 * @author Federico Lombardi - Sapienza University of Rome
 *
 */
public class Profiler {
	
	private static final Logger logger = Logger.getLogger(Profiler.class);
	
	private static final String PROP_FILENAME = "profiler.properties";
	
	private static final String LOAD_CSV = "profiledLoad.csv";
	private static final String ALFA_CSV = "profiledAlfa.csv";
	private static final String TUPLE_SIZE_CSV = "profiledTuple.csv";
	private static final String WORKER_CSV = "profiledWorker.csv";
	private static final String TOPOLOGY_SER = "topology.ser";
	
	private static boolean PROFILE_REGRESSION;
	private static boolean PROFILE_SELECTIVITY;
	private static boolean PROFILE_TUPLE_SIZE;
	private static boolean PROFILE_CPU_WORKER;
	
	private static String IP_ADDRESS;         // ip address of nimbus (or localhost if tunneling)
	private static int ITERATION_NUMBER;      // number of profiling iterations
	private static int ITERATION_DURATION;    // iteration duration in seconds -> total profiling duration = ITERATION_NUMBER * ITERATION_DURATION
	private static String[] WORKER_HOSTNAMES;
	
	private static boolean STOP = false;
	
	/**
	 * Path in which store CSV files with profiled results
	 * (redundant, the results are stored in topology as well)
	 */
	private static String PATH = "resources/"; // default value
	
	private static long PROFILING_ID;

	/**
	 * Call the related methods to set the definitive tables
	 * @param t the input profiled topology
	 * @return the topology with the definitive tables set
	 * @throws Exception
	 */
	private static Topology setDefinitiveTables(Topology t) throws Exception {
		logger.info("Setting definitive tables..");
		
		if (PROFILE_SELECTIVITY)
			t = setDefinitiveAlfa(t);
		
		if (PROFILE_REGRESSION)
			t = setDefinitiveRegressionFunctions(t);
		
		if (PROFILE_TUPLE_SIZE)
			t = setDefinitiveTupleSize(t);
		
		if (PROFILE_CPU_WORKER)
			t = setDefinitiveWorkerCpuUsage(t);
			
		logger.info("Definitive tables set succesfully");
		return t;	
	}
	
	/**
	 * Set the definitive alfa table
	 * @param t the input topology
	 * @return an updated topology
	 */
	private static Topology setDefinitiveAlfa(Topology t){
		logger.info("Setting definitive alfa and max internal traffic for each edge..");
		t.setAlfaTable();
		
		Set<DefaultWeightedEdge> edges = t.getGraph().edgeSet();
		logger.debug("Print Alfa avg..");
		for (DefaultWeightedEdge currentEdge : edges) {
			logger.debug("\tedge "+currentEdge.toString()+", alfa="+t.getAlfaTable().get(currentEdge));
		}
		
		logger.info("Definitive Alfa and max internal traffic set succesfully for all edges.");
		System.out.println();
		return t;
	}
	
	/**
	 * Set for each edge the definitive regression function as a neural network
	 * @param t the input topology
	 * @return an updated topology
	 * @throws FileNotFoundException
	 */
	private static Topology setDefinitiveRegressionFunctions(Topology t) throws FileNotFoundException{
		logger.info("Setting definitive regression functions for each component..");
		t.setLoadTable(PROFILING_ID);
		
		logger.debug("Print load history:");
		Set<Component> components = t.getGraph().vertexSet();
		for (Component currentComponent : components) {
			logger.debug("\tcomponent "+currentComponent.toString()+", load="+t.getLoadTable().get(currentComponent)+", loadArray="+Utils.printArray(t.getLoadTableHistory().get(currentComponent)));
		}
		
		logger.info("Definitive regression functions set succesfully for each component.");
		System.out.println();
		return t;
	}
	
	/**
	 * Set the definitive tuple size table
	 * @param t the input topology
	 * @return an updated topology
	 */
	private static Topology setDefinitiveTupleSize(Topology t){
		logger.info("Setting definitive tuple-size for each edge..");
		t.setTupleSizeTable();
		
		logger.debug("Print avg tuple-size:");
		Set<DefaultWeightedEdge> edges = t.getGraph().edgeSet();
		for (DefaultWeightedEdge currentEdge : edges) {
			logger.debug("\tedge "+currentEdge.toString()+", tuple size="+t.getTupleSizeTable().get(currentEdge));
		}
		
		logger.info("Definitive tuple-size set succesfully for each edge.");
		System.out.println();
		return t;
	}
	
	/**
	 * Set the definitive worker cpu usage function
	 * @param t the input topology
	 * @return an updated topology
	 * @throws Exception 
	 */
	private static Topology setDefinitiveWorkerCpuUsage(Topology t) throws Exception{
		logger.info("Setting definitive worker cpu usage function..");
		t.setWorkerCpuUsage();
		
		logger.info("Definitive worker cpu usage function set succesfully.");
		System.out.println();
		return t;
	}
	
	/**
	 * Profile an iteration: Step 1: regression, selectivity, tuple size. Step 2: worker cpu. Step 3: update topology
	 * @param t input topology
	 * @param outLoad output stream for writing load
	 * @param outAlfa output stream for writing alfa
	 * @param outLoad output stream for writing tuple-size
	 * @param outWorker output stream for writing worker cpu usage
	 * @param iter iteration
	 * @return an updated topology
	 * @throws Exception 
	 */
	private static Topology profilingIteration(Topology t, PrintWriter outLoad, PrintWriter outAlfa, PrintWriter outTuple, PrintWriter outWorker, int iter) throws Exception{
		logger.info("Profiling iteration "+iter+"..");
		
		// STEP1: profile per-component: 1) regression, 2) selectivity, 3) tuple-size (optional), 4) worker cpu
		ListenableDirectedWeightedGraph<Component,DefaultWeightedEdge> g = t.getGraph();
		Set<Component> components = g.vertexSet();
		for (Component currentComponent : components) {
			// 1.1) profile regression with input traffic (component=bolt) or output traffic (component=spout) 
			if (PROFILE_REGRESSION) {
				logger.info("Profiling regression for component "+currentComponent+"..");
				t = RegressionFunctProfiler.profileRegression(t, currentComponent, iter, outLoad);
			}	
			
			// 1.2) profile selectivity
			if (PROFILE_SELECTIVITY) {
				logger.info("Profiling selectivity for component "+currentComponent+"..");
				t = SelectivityProfiler.profileSelectivity(t, currentComponent, iter, outAlfa);
			}
			
			// 1.3) profile tuple-size (optional)
			if (PROFILE_TUPLE_SIZE) {
				logger.debug("Profiling tuple-size for component "+currentComponent+"..");
				t = TupleSizeProfiler.profileTupleSize(t, currentComponent, iter, outTuple);
			}
			
		}
		
		// STEP2: profile per-worker CPU usage
		if (PROFILE_CPU_WORKER) {
			logger.info("Profiling CPU usage for worker array..");
			WorkerCpuProfiler.profileWorkerCPuUsage(t, WORKER_HOSTNAMES, iter, outWorker);
		}
		
		// STEP3: update graph and topology
		g = GraphUtils.updateWeight(g);
		t.updateGraphInTopology(g);
		
		logger.info("Iteration "+iter+" completed.\n");
		return t;
	}
	
	/**
	 * Start the profiling phase
	 * @param t the input topology
	 * @param outLoad
	 * @param outAlfa 
	 * @param outTuple
	 * @param outWorker
	 * @param iter
	 * @return an updated topology
	 * @throws Exception 
	 */
	private static Topology startProfiling(Topology t, PrintWriter outLoad, PrintWriter outAlfa, PrintWriter outTuple, PrintWriter outWorker) throws Exception{
		logger.info("Start profiling for " + ITERATION_NUMBER + " iterations..");
		int iter = 0;
		while (iter < ITERATION_NUMBER && !STOP) {
			t = profilingIteration(t, outLoad, outAlfa, outTuple, outWorker, iter);
			++iter;			
			Thread.sleep(ITERATION_DURATION * 1000); // wait ITERATION_DURATION seconds
		}
		
		outLoad.close();
		outAlfa.close();
		outTuple.close();
		outWorker.close();
		
		logger.info("Profiling iterations completed.");
		return t;
	}
	
	/**
	 * Serialize on file the topology
	 * @param t input topology to serialize
	 */
	public static void serializeTopology(Topology t){
		logger.info("Serializing topology on file..");
		try {
			String path = PATH+"topology_ser/"; 
			FileOutputStream fout = new FileOutputStream(path+TOPOLOGY_SER.replaceFirst(".csv", "." + PROFILING_ID +".csv"));
			ObjectOutputStream oos = new ObjectOutputStream(fout);   
			oos.writeObject(t);
			oos.close();
			logger.info("Topology successfully serialized in "+path+TOPOLOGY_SER.replaceFirst(".csv", "." + PROFILING_ID +".csv"));
		} catch(Exception ex) {
			   logger.error("An error occurs while serializing topology.", ex);
		}
	}
	
	
	/**
	 * Main method to start profiling
	 * @param args no arguments needed
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception{
				
		DOMConfigurator.configure("properties/log4j.xml");
		
		// load properties file to create output files and read the number of iteration of profiling
		//String propFileName = "topology.properties";
		Properties prop = new Properties();
		InputStream inputStream = SelectivityProfiler.class.getClassLoader().getResourceAsStream(PROP_FILENAME);
		try {
			prop.load(inputStream);
		} catch (IOException e) {
			logger.error("Some problem occur while reading property file '" + PROP_FILENAME + "'. It might be not found in the classpath");	
			e.printStackTrace();
		}
		
		// get which parts profile from properties
		PROFILE_REGRESSION = Boolean.valueOf(prop.getProperty("profileRegression").toString());
		PROFILE_SELECTIVITY = Boolean.valueOf(prop.getProperty("profileSelectivity"));
		PROFILE_TUPLE_SIZE = Boolean.valueOf(prop.getProperty("profileTupleSize"));
		PROFILE_CPU_WORKER = Boolean.valueOf(prop.getProperty("profileCpuWorker"));
		logger.info("Properties loaded. PROFILE_REGRESSION = " + PROFILE_REGRESSION + ", PROFILE_SELECTIVITY = " + PROFILE_SELECTIVITY + ", PROFILE_TUPLE_SIZE = " + PROFILE_TUPLE_SIZE + ", PROFILE_CPU_WORKER = " + PROFILE_CPU_WORKER);
		
		// get other properties
		PATH = prop.getProperty("path");
		IP_ADDRESS = prop.getProperty("ip");
		if (IP_ADDRESS.contains("local")) // hint: ssh tunnling is slow, so no wait in that case!
			ITERATION_DURATION = 0;
		else
			ITERATION_DURATION = Integer.parseInt(prop.getProperty("profilingIterationDuration"));
		
		ITERATION_NUMBER = Integer.parseInt(prop.getProperty("profilingIterationNumber"));
		WORKER_HOSTNAMES = prop.getProperty("worker_hostnames").split(",");
		logger.info("Cluster size: " + WORKER_HOSTNAMES.length + ", iterations: " + ITERATION_NUMBER + ", path: " + PATH);
		
		// set profiling id- This value will be written on LOAD_CSV, ALFA_CSV, TUPLE_SIZE and WORKER_CSV
		PROFILING_ID = System.currentTimeMillis();
		
		// output files
		File fileLoad = new File(PATH+"profiled_values", LOAD_CSV.replaceFirst(".csv", "." + PROFILING_ID +".csv"));
		File fileAlfa = new File(PATH+"profiled_values", ALFA_CSV.replaceFirst(".csv", "." + PROFILING_ID +".csv"));
		File fileTuple = new File(PATH+"profiled_values", TUPLE_SIZE_CSV.replaceFirst(".csv", "." + PROFILING_ID +".csv"));
		File fileWorker = new File(PATH+"profiled_values", WORKER_CSV.replaceFirst(".csv", "." + PROFILING_ID +".csv"));
		PrintWriter outLoad = new PrintWriter(fileLoad);
		PrintWriter outAlfa = new PrintWriter(fileAlfa);
		PrintWriter outTuple = new PrintWriter(fileTuple);
		PrintWriter outWorker = new PrintWriter(fileWorker);
		
		// topology initialization
		logger.info("Initializing topology..");
		Topology t = TopologyBuilder.initializeTopology();
		
		/*
		Runtime.getRuntime().addShutdownHook(new Thread()
        {
            @Override
            public void run()
            {
                System.out.println("Shutdown hook ran! Setting STOP variable to 'true'...");
                STOP = true;
            }
        });
		*/
		
		// start profiling phase
		t = startProfiling(t, outLoad, outAlfa, outTuple, outWorker);
		
		// set definitive tables
		t = setDefinitiveTables(t);
		
		// serialize topology on file
		serializeTopology(t);
		
		logger.info("Profiling completed.");
		System.exit(0);
	}

}
