package midlab.storm.autoscaling.topology;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

import midlab.storm.autoscaling.database.DatabaseConnection;
import midlab.storm.autoscaling.profiler.SelectivityProfiler;

import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;
import org.encog.engine.network.activation.ActivationTANH;
import org.encog.ml.data.MLDataSet;
import org.encog.neural.networks.BasicNetwork;
import org.encog.neural.networks.layers.BasicLayer;
import org.encog.neural.networks.training.Train;
import org.encog.neural.networks.training.propagation.resilient.ResilientPropagation;
import org.encog.persist.EncogDirectoryPersistence;
import org.encog.util.csv.CSVFormat;
import org.encog.util.simple.TrainingSetUtil;
import org.jgrapht.graph.DefaultWeightedEdge;
import org.jgrapht.graph.ListenableDirectedWeightedGraph;

/**
 * Profiled Storm Topology
 * @author Federico Lombardi - Sapienza University of Rome
 *
 */
public class Topology implements Serializable{

	private static final Logger logger = Logger.getLogger(Topology.class);
	
	/**
	 * serial version for topology serialization
	 */
	private static final long serialVersionUID = -7710117478374627536L;
	
	/**
	 * name of property file name with the characteristics of topology
	 */
	public static final String PROP_FILENAME = "topology.properties";
	
	/**
	 * topology name
	 */
	private String name;
	
	/**
	 * parent folder (usaually resources/) on which store the directory dataset/ and neural_networks
	 */
	private String PATH;
	
	/**
	 * topology graph
	 */
	private ListenableDirectedWeightedGraph<Component,DefaultWeightedEdge> graph;
	
	/**
	 * gui representation of topology graph
	 */
	private ListenableDirectedWeightedGraph<String,DefaultWeightedEdge> graphGui;
	
	
	/************************************************************************************/
	
	/** TEMPORANEY TABLES */
	
	/**
	 * structure: Edge, [alfa history]
	 */
	private Hashtable<DefaultWeightedEdge,Double[]> alfaTableHistory;
	
	/**
	 * structure: Component, [tot_stream_in_component , load history] 
	 */
	private Hashtable<Component,String[]> loadTableHistory;
	
	/**
	 * structure: Edge, [tuple_size history]
	 */
	private Hashtable<DefaultWeightedEdge,Double[]> tupleSizeTableHistory;
	
	/**
	 * structure: [sum(input traffic of each component), sum(output traffic of each component), worker cpu %]
	 */
	private String[] workerCpuUsageHistory;
	
	
	/************************************************************************************/
	
	/** DEFINITIVE TABLES **/
	
	/**
	 * DEFINITIVE ALFA - structure: Edge , Alfa avg on that edge
	 */
	private Hashtable<DefaultWeightedEdge,Double> alfaTable;
	
	/**
	 * DEFINITIVE LOAD - structure: Component , ANN
	 */
	private Hashtable<Component,BasicNetwork> loadTable;
	
	/**
	 * DEFINITIVE TUPLE SIZE - structure: Edge , Tuple_Size avg on that edge
	 */
	private Hashtable<DefaultWeightedEdge,Double> tupleSizeTable;
	
	/**
	 * structure: Edge , Max_Traffic on that edge
	 */
	private Hashtable<DefaultWeightedEdge,Double> maxTrafficTable;
	
	
	/************************************************************************************/
	
	/**
	 * Neural Network to infer percentage of cpu utilization given the sum of input traffic, output traffic of its executor
	 */
	private BasicNetwork workerCpuUsageNetwork;
	
	/** TRF, MAX CPU, MAX TRAFFIC VALUES **/
	
	/**
	 * Tweet Replication Factor (Scale Factor)
	 */
	private int TRF;
	
	/**
	 * the total max cpu load represented as core_speed*num_core
	 */
	private long MAX_CPU_LOAD;
	
	/**
	 * the max traffic (tuple/min) emitted from a spout (without considering trf)
	 */
	private double MAX_TRAFFIC_MIN;
	
	/**
	 * the max traffic (tuple/sec) emitted from a spout (without considering trf)
	 */
	private double MAX_TRAFFIC_SEC;
	
	/**
	 * max traffic (tuple/sec) in a worker. It will be initialized in initializeWorkerNetwork method
	 */
	private double MAX_TRAFFIC_SEC_IN_WORKER;
	
	
	/************************************************************************************/
	
	/** CONSTRUCTORS METHODS **/
	
	/**
	 * Build an object topology with a graph of Components
	 * @param name topology name
	 * @param graph input graph
	 * @throws Exception 
	 */
	public Topology(String name, ListenableDirectedWeightedGraph<Component,DefaultWeightedEdge> graph) throws Exception{
		DOMConfigurator.configure("properties/log4j.xml");
		this.name = name;
		this.graph = graph;
		this.alfaTableHistory = new Hashtable<DefaultWeightedEdge, Double[]>();
		this.loadTableHistory = new Hashtable<Component,String[]>();
		this.tupleSizeTableHistory = new Hashtable<DefaultWeightedEdge, Double[]>();
		this.alfaTable = new Hashtable<DefaultWeightedEdge, Double>();
		this.loadTable = new Hashtable<Component,BasicNetwork>();
		this.tupleSizeTable = new Hashtable<DefaultWeightedEdge, Double>();
		this.maxTrafficTable = new Hashtable<DefaultWeightedEdge,Double>();

		//setting max cpu load by reading from the db the max cpu value
		setMaxCpuLoad(readMaxCpuLoad());
		
		//opening properties file to read max traffic value
		Properties prop = new Properties();
		InputStream inputStream = SelectivityProfiler.class.getClassLoader().getResourceAsStream(PROP_FILENAME);
		try {
			prop.load(inputStream);
		} catch (IOException e) {
			logger.error("property file '" + PROP_FILENAME + "' not found in the classpath");	
			e.printStackTrace();
		}

		//setting parent folder
		this.PATH = prop.getProperty("path");
		
		//setting TRF, MAX_TRAFFIC_MIN (tuple/min), MAX_TRAFFIC_SEC (tuple/sec) read into properties file
		this.TRF = Integer.parseInt(prop.getProperty("trf"));
		this.MAX_TRAFFIC_MIN = Double.parseDouble(prop.getProperty("maxTraffic")) * (double)this.TRF;
		this.MAX_TRAFFIC_SEC = this.MAX_TRAFFIC_MIN / 60D;
		this.MAX_TRAFFIC_SEC_IN_WORKER = 0D;
	}
	
	/**
	 * Build an onject Topology with a graph and a gui graph
	 * @param name topology name
	 * @param graph input graph
	 * @param graphGui input gui graph
	 * @throws Exception 
	 */
	public Topology(String name, ListenableDirectedWeightedGraph<Component,DefaultWeightedEdge> graph, ListenableDirectedWeightedGraph<String,DefaultWeightedEdge> graphGui) throws Exception{
		DOMConfigurator.configure("properties/log4j.xml");
		this.name = name;
		this.graph = graph;
		this.graphGui = graphGui;
		this.alfaTableHistory = new Hashtable<DefaultWeightedEdge, Double[]>();
		this.loadTableHistory = new Hashtable<Component,String[]>();
		this.tupleSizeTableHistory = new Hashtable<DefaultWeightedEdge, Double[]>();
		this.alfaTable = new Hashtable<DefaultWeightedEdge, Double>();
		this.loadTable = new Hashtable<Component,BasicNetwork>();
		this.tupleSizeTable = new Hashtable<DefaultWeightedEdge, Double>();
		this.maxTrafficTable = new Hashtable<DefaultWeightedEdge,Double>();
		
		//setting max cpu load by reading from the db the max cpu value
		setMaxCpuLoad(readMaxCpuLoad());
		
		//opening properties file to read max traffic value
		Properties prop = new Properties();
		InputStream inputStream = SelectivityProfiler.class.getClassLoader().getResourceAsStream(PROP_FILENAME);
		try {
			prop.load(inputStream);
		} catch (IOException e) {
			logger.error("property file '" + PROP_FILENAME + "' not found in the classpath");	
			e.printStackTrace();
		}
		
		//setting parent folder
		this.PATH = prop.getProperty("path");
				
		//setting TRF, MAX_TRAFFIC_MIN (tuple/min), MAX_TRAFFIC_SEC (tuple/sec) read into properties file
		this.TRF = Integer.parseInt(prop.getProperty("trf"));
		this.MAX_TRAFFIC_MIN = Double.parseDouble(prop.getProperty("maxTraffic")) * (double)this.TRF;
		this.MAX_TRAFFIC_SEC = MAX_TRAFFIC_MIN / 60D;
		this.MAX_TRAFFIC_SEC_IN_WORKER = 0D;
	}
		
	
	/************************************************************************************/
	
	/** GET METHODS CALLED FROM FORECASTER  **/
	
	/**
	 * Return the name of the topology
	 * @return the string containing the topology name
	 */
	public String getName(){
		return this.name;
	}
	
	/**
	 * Return the scale factor (Tweet Replication Factor)
	 * @return int value of trf
	 */
	public int getTrf(){
		return this.TRF;
	}
	
	/**
	 * Return the max load value
	 * @return a long containing the max load value
	 */
	public long getMaxCpuLoad(){
		return this.MAX_CPU_LOAD;
	}
	
	/**
	 * Return the max traffic per minute (tuple/min). NOTE: it is already multiplied * trf in the constructor method
	 * @return max tuple/min
	 */
	public double getMaxTrafficPerMinute(){
		return this.MAX_TRAFFIC_MIN;
	}
	
	/**
	 * Return the max traffic per second (tuple/sec)
	 * @return max tuple/sec
	 */
	public double getMaxTrafficPerSecond(){
		return this.MAX_TRAFFIC_SEC;
	}
		
	/**
	 * Return the component graph
	 * @return the graph of components
	 */
	public ListenableDirectedWeightedGraph<Component,DefaultWeightedEdge> getGraph(){
		return this.graph;
	}
	
	/**
	 * Return the gui graph
	 * @return the gui graph
	 */
	public ListenableDirectedWeightedGraph<String,DefaultWeightedEdge> getGraphGui(){
		return this.graphGui;
	}
	
	/**
	 * Return a component by giving its name
	 * @param name the component name
	 * @return the component
	 */
	public Component getComponent(String name){
		Set<Component> components = graph.vertexSet();
		Iterator<Component> itComponent = components.iterator();
		Component currentComponent;
		while(itComponent.hasNext()){
			currentComponent = itComponent.next();
			if(currentComponent.toString().equals(name)) return currentComponent;
		}
		logger.error("impossible to find a component named '"+name+"'. ");
		return null;
	}
	
	/**
	 * Return the list of all components
	 * @return
	 */
	public Set<Component> getComponents() {
		return graph.vertexSet();
	}
	
	/**
	 * Return a list of spouts
	 * @return a set containing the spouts
	 */
	public Set<Bolt> getBolts(){
		Set<Component> components = graph.vertexSet();
		Set<Bolt> bolts = new HashSet<Bolt>();
		Iterator<Component> itComponent = components.iterator();
		Component currentComponent;
		while(itComponent.hasNext()){
			currentComponent = itComponent.next();
			if(currentComponent.isBolt()) bolts.add((Bolt)currentComponent);
		}
		return bolts;
	}
	
	/**
	 * Return a list of spouts
	 * @return a set containing the spouts
	 */
	public Set<Spout> getSpouts(){
		Set<Component> components = graph.vertexSet();
		Set<Spout> spouts = new HashSet<Spout>();
		Iterator<Component> itComponent = components.iterator();
		Component currentComponent;
		while(itComponent.hasNext()){
			currentComponent = itComponent.next();
			if(currentComponent.isSpout()) spouts.add((Spout)currentComponent);
		}
		return spouts;
	}
	
	/**
	 * Return a set of sink nodes i.e. vertexes of the graph that have at least 1 input edge, but 0 output edges
	 * @return a set of sink nodes
	 */
	public Set<Component> getSinkNodes(){
		Set<Component> components = graph.vertexSet();
		Iterator<Component> itComponent = components.iterator();
		Set<Component> sinkNodes = new HashSet<Component>();
		while(itComponent.hasNext()){
			Component currentComponent = itComponent.next();
			//if the current component is a sink node
			if(graph.outgoingEdgesOf(currentComponent).isEmpty() && (!graph.incomingEdgesOf(currentComponent).isEmpty()) ){
				//log4j.debug("sink node: "+currentComponent);
				sinkNodes.add(currentComponent);
			}
		}
		return sinkNodes;
	}
	
	/**
	 * Return the alfa history table
	 * @return an hashtable with the history of alfa for each edge
	 */
	public Hashtable<DefaultWeightedEdge,Double[]> getAlfaTableHistory(){
		return this.alfaTableHistory;
	}
	
	/**
	 * Return the load history table
	 * @return an hashtable with the history of load for each vertex
	 */
	public Hashtable<Component,String[]> getLoadTableHistory(){
		return this.loadTableHistory;
	}

	/**
	 * Return the tuple size history table
	 * @return an hashtable with the history of tuple size for each edge
	 */
	public Hashtable<DefaultWeightedEdge,Double[]> getTupleSizeTableHistory(){
		return this.tupleSizeTableHistory;
	}
	
	/**
	 * Return the history array of workers cpu usage
	 * @return an array containing the history in form [inputTraffic, outputTraffic, Load]; input/output traffic of its executors
	 */
	public String[] getWorkerCpuUsageHistory(){
		return this.workerCpuUsageHistory;
	}
	
	/**
	 * Return the definite alfa table
	 * @return the definite hashtable
	 */
	public Hashtable<DefaultWeightedEdge,Double> getAlfaTable(){
		return this.alfaTable;
	}
	
	/**
	 * Return the definitive load table
	 * @return the definitive hashtable
	 */
	public Hashtable<Component,BasicNetwork> getLoadTable(){
		return this.loadTable;
	}	

	/**
	 * Return the definitive tuple size table
	 * @return the definitive hashtable
	 */
	public Hashtable<DefaultWeightedEdge,Double> getTupleSizeTable(){
		return this.tupleSizeTable;
	}
	
	/**
	 * Return the max traffic table (tuple/sec)
	 * @return the max traffic table (tuple/sec)
	 */
	public Hashtable<DefaultWeightedEdge, Double> getMaxTrafficTable(){
		return this.maxTrafficTable;
	}
	
	/**
	 * Return for a given component the max input traffic (tuple/sec)
	 * @param c the component
	 * @return max input traffic (tuple/sec)
	 */
	public double getMaxTrafficIn(Component c){
 		double maxTrafficIn = 0.0;
 		Set<DefaultWeightedEdge> inEdges = graph.incomingEdgesOf(c);
 		Iterator<DefaultWeightedEdge> itInEdge = inEdges.iterator();
 		while(itInEdge.hasNext()){
 			maxTrafficIn += maxTrafficTable.get(itInEdge.next());
 		}
 		return maxTrafficIn;
 	}
	
	/**
	 * Return for a given component the current input traffic (tuple/sec)
	 * @param c the component
	 * @return input traffic (tuple/sec)
	 */
	public double getTrafficIn(Component c){
		ListenableDirectedWeightedGraph<Component,DefaultWeightedEdge> g = this.graph;
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
				weight = g.getEdgeWeight(currentEdge);
				//log4j.info("traffic from spout: "+weight);
			}
			else{
				alfa = alfaTable.get(currentEdge);
				weight = alfa * getTrafficIn(cSource);
				//log4j.info("edge "+currentEdge.toString()+": alfa="+alfa+" weight="+weight);
			}
			inWeight += weight;
		}
		return inWeight;
	}
	
	
	/************************************************************************************/
	
	/** SET METHODS CALLED FROM PROFILER  **/
	
	/**
	 * Update the Alfa Table with a new one
	 * @param newAlfaTable
	 */
	public void setAlfaTableHistory(Hashtable<DefaultWeightedEdge,Double[]> newAlfaTableHistory){
		this.alfaTableHistory = newAlfaTableHistory;
	}
	
	/**
	 * Update the Load Table with a new one
	 * @param newLoadTable
	 */
	public void setLoadTableHistory(Hashtable<Component,String[]> newLoadTableHistory){
		this.loadTableHistory = newLoadTableHistory;
	}

	/**
	 * Update the Tuple Size Table with a new one
	 * @param newTupleSizeTable
	 */
	public void setTupleSizeTableHistory(Hashtable<DefaultWeightedEdge,Double[]> newTupleSizeTableHistory){
		this.tupleSizeTableHistory = newTupleSizeTableHistory;
	}
	
	/**
	 * 
	 * @param newWorkerCpuUsageHistory
	 */
	public void setWorkerCpuUsageHistory(String[] newWorkerCpuUsageHistory){
		this.workerCpuUsageHistory = newWorkerCpuUsageHistory;
	}
	
	/**
	 * Call this method when profiling phase done to set the alfa table. This method automatically
	 * call the method to set the internal max traffic (tuple/sec)
	 */
	public void setAlfaTable(){
		//setting alfa table
		Set<DefaultWeightedEdge> edges = alfaTableHistory.keySet();
		Iterator<DefaultWeightedEdge> itEdge = edges.iterator();
		DefaultWeightedEdge currentEdge;
		Double[] currentAlfaArray;
		while(itEdge.hasNext()){
			currentEdge = itEdge.next();
			//currentAlfaArray = trimFirstAlfa(currentEdge);
			currentAlfaArray = alfaTableHistory.get(currentEdge);
			alfaTable.put(currentEdge, avgCalculate(currentAlfaArray));
		}
		
		//start for each sink node and compute max internal traffic graph
		Set<Component> sinkNodes = getSinkNodes();
		Iterator<Component> itSinkNode = sinkNodes.iterator();
		while(itSinkNode.hasNext()){
			Component currentComponent = itSinkNode.next();
			setInternalMaxTrafficIn(currentComponent);
		}
	}
		
	/**
	 * Call this method when profiling phase done to set the load table
	 * @throws FileNotFoundException
	 */
	public void setLoadTable(long profilingId) throws FileNotFoundException{
		Set<Component> components = loadTableHistory.keySet();
		Iterator<Component> itComponent = components.iterator();
		Component currentComponent;
		while(itComponent.hasNext()){
			currentComponent = itComponent.next();
			logger.info("Building neural network for " + currentComponent.toString());
			BasicNetwork network = initializeComponentNetwork(currentComponent, profilingId);
			loadTable.put(currentComponent, network);
		}
	}

	/**
	 * Call this method when profiling phase done to set the tuple size table
	 */
	public void setTupleSizeTable(){
		Set<DefaultWeightedEdge> edges = tupleSizeTableHistory.keySet();
		Iterator<DefaultWeightedEdge> itEdge = edges.iterator();
		DefaultWeightedEdge currentEdge;
		Double[] currentTupleSizeArray;
		while(itEdge.hasNext()){
			currentEdge = itEdge.next();
			currentTupleSizeArray = tupleSizeTableHistory.get(currentEdge);
			tupleSizeTable.put(currentEdge, avgCalculate(currentTupleSizeArray));
		}
	}

	/**
	 * Set the neural network for infer worker cpu usage percentage
	 * @throws Exception
	 */
	public void setWorkerCpuUsage() throws Exception{
		initializeWorkerNetwork();
	}
	
 	/**
 	 * Set the max CPU load read from the database
 	 * @param totLoad read from db with readMaxCpuLoad()
 	 */
 	public void setMaxCpuLoad(long load){
 		this.MAX_CPU_LOAD = load;
 	}
	
	/**
	 * Update the topology graph with a new one
	 * @param g the new graph
	 */
 	public void updateGraphInTopology(ListenableDirectedWeightedGraph<Component,DefaultWeightedEdge> g){
		this.graph = g;
	}
 	
 	/**
 	 * Update a neural network for a given component
 	 * @param componentName
 	 * @param network
 	 */
 	public void updateNeuralNetworkInComponent(String componentName, BasicNetwork network){
 		Set<Component> components = loadTable.keySet();
		Iterator<Component> itComponent = components.iterator();
		Component currentComponent;
		while(itComponent.hasNext()){
			currentComponent = itComponent.next();
			if(currentComponent.getName().equals(componentName)){
				logger.debug("updating neural network for "+currentComponent.toString());
				loadTable.remove(currentComponent);
				loadTable.put(currentComponent, network);
				break;
			}
		}
		logger.debug("neural network for "+componentName+" updated succesfully.");
 	}
 	
 	/**
 	 * Update the maxTraffic (req/min) and trf; if you want to update only one variable, just pass a negative number or zero
 	 * @param maxTraffic the new maxTraffic (req/min); 0 or negative number to let the current value
 	 * @param trf the new trf; 0 or negative number to let the current value
 	 */
 	public void updateMaxTrafficAndTrf(double maxTraffic, int trf) {
 		if (trf > 0)
 			this.TRF = trf;
 		
 		if (maxTraffic > 0) {
 			this.MAX_TRAFFIC_MIN = maxTraffic * (double)this.TRF;
 			this.MAX_TRAFFIC_SEC = this.MAX_TRAFFIC_MIN / 60D;
 		}
 		
 		// update alfa table to compute the new max traffic value for each component
 		setAlfaTable();
 		
 		// update load table to compute the new normalization factor for each component
 		try {
			setLoadTable(System.currentTimeMillis());
		} catch (FileNotFoundException e) {
			logger.error("Impossible to update the Load Table", e);
		}
 	}
 	
 	@SuppressWarnings("unused")
 	public void updateTaskList(Topology t2){
 		ArrayList<Component> oldComponents = new ArrayList<Component>();
 		ArrayList<Component> newComponents = new ArrayList<Component>();
 		oldComponents.addAll(graph.vertexSet());
 		newComponents.addAll(t2.getGraph().vertexSet());
 		
 		ArrayList <DefaultWeightedEdge> oldEdges = new ArrayList<DefaultWeightedEdge>();
 		ArrayList <DefaultWeightedEdge> newEdges = new ArrayList<DefaultWeightedEdge>();
 		oldEdges.addAll(graph.edgeSet());
 		oldEdges.addAll(t2.getGraph().edgeSet());
 		
 		Hashtable<DefaultWeightedEdge, Double[]> newAlfaTableHistory = new Hashtable<DefaultWeightedEdge, Double[]>();
 		Hashtable<Component, String[]> newLoadTableHistory = new Hashtable<Component, String[]>();
 		Hashtable<DefaultWeightedEdge, Double[]> newTupleSizeHistory = new Hashtable<DefaultWeightedEdge, Double[]>();
 		Hashtable<DefaultWeightedEdge, Double> newAlfaTable = new Hashtable<DefaultWeightedEdge, Double>();
 		Hashtable<Component, BasicNetwork> newLoadTable = new Hashtable<Component, BasicNetwork>();
 		Hashtable<DefaultWeightedEdge, Double> newTupleSizeTable = new Hashtable<DefaultWeightedEdge, Double>();
 		Hashtable<DefaultWeightedEdge, Double> newMaxTrafficTable = new Hashtable<DefaultWeightedEdge, Double>();
 		
 		for(Component c : oldComponents){
 			//prendo il nuovo component usando il nome del vecchio component
 			Component newC = newComponents.get(oldComponents.indexOf(c));
 			
 			//inserisco gli stessi valori della load table history e della
 			//load table del vecchio component
 			newLoadTableHistory.put(newC, loadTableHistory.get(c));
 			newLoadTable.put(newC, loadTable.get(c));
 		}
 		
 		for(DefaultWeightedEdge e : oldEdges){
 			//prendo il nuovo arco usando i riferimenti agli estremid el vecchio
 			Component cSource = graph.getEdgeSource(e);
 			Component cDest = graph.getEdgeTarget(e);
 			
 			//cSource = newComponents.get(newComponents.indexOf(o));
 			DefaultWeightedEdge newE = newEdges.get(oldEdges.indexOf(graph.getEdge(cSource, cSource)));
 			
 		}
 		
 	}
 	
 	@Override
 	public String toString(){
 		String s = "Component List:\n";
 		
 		Set<Component> components = graph.vertexSet();
 		Set<DefaultWeightedEdge> edges = graph.edgeSet();
 		for(Component c : components){
 			s += c.getName()+", task: "+c.getTasks().toString()+"\n";
 		}
 		
 		s+= "Edges List:\n";
 		for(DefaultWeightedEdge e : edges){
 			s += e.toString();
 		}
 		
 		return s;
 	}
 	
 	/**
	 * Called during construction: Read from db the max CPU load summing of each worker core_speed*num_core
	 * @throws Exception
	 * @return totLoad = core_speed*num_core
	 */
	private long readMaxCpuLoad() throws Exception{
		long totLoad = 0;
		String query = "select CORE_COUNT,CORE_SPEED from APP.NODE";
		//System.out.println(query);
		query = query.replaceAll("'", "");
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		conn = DatabaseConnection.connect();
		ps = conn.prepareStatement(query);
		rs = ps.executeQuery();
		while (rs.next()){
			int coreCont = Integer.parseInt(rs.getString(1));
			long coreSpeed = Long.parseLong(rs.getString(2));
			totLoad += coreCont*coreSpeed;
			//System.out.println("adding load "+rs.getString(1));
		}

		if (conn!=null)
			conn.close();
		if (ps!=null)
			ps.close();
		if (rs!=null)
			rs.close();
		
		return totLoad;
	}

	/**
	 * Set the max input traffic (tuple/sec) for a given Component
	 * @param c the component
	 * @return the max input traffic
	 */
	private double setInternalMaxTrafficIn(Component c){
		ListenableDirectedWeightedGraph<Component,DefaultWeightedEdge> g = this.graph;
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
				weight = MAX_TRAFFIC_SEC;
			}
			else{
				alfa = this.alfaTable.get(currentEdge);
				weight = alfa * setInternalMaxTrafficIn(cSource);
			}
			this.maxTrafficTable.put(currentEdge, weight);
			inWeight += weight;
		}
		return inWeight;
	}
		
	/**
	 * Initialize the ANN to infer load from traffic for a given component
	 * @param currentComponent
	 * @return the ANN
	 * @throws FileNotFoundException
	 */
	private BasicNetwork initializeComponentNetwork(Component currentComponent, long profilingId) throws FileNotFoundException{
		
		//load the loadTableHisotry of the current component in an array
		//String[] currentTrafficLoadArray = trimFirstLoad(currentComponent);
		String[] currentTrafficLoadArray = loadTableHistory.get(currentComponent);
		
		//builds a network
		BasicNetwork network = new BasicNetwork();
		network.addLayer(new BasicLayer(new ActivationTANH(), true, 1));
		network.addLayer(new BasicLayer(new ActivationTANH(), true, 3));
		network.addLayer(new BasicLayer(new ActivationTANH(), true, 1));
		network.getStructure().finalizeStructure();
		network.reset();
		
		//creating the neural network file and the training file with the related print writer
		String currentComponentNameSuffix = currentComponent.toString() + "." + profilingId;
		String trainFileName = PATH + "dataset/train_" + currentComponentNameSuffix + ".csv";
		String networkFileName = PATH + "neural_networks/net_" + currentComponentNameSuffix + ".eg";
		File trainFile = new File(trainFileName);
		File networkFile = new File(networkFileName);
		PrintWriter out = new PrintWriter(trainFile);
		
		//finding the max input traffic (tuple/sec) for the current component. It will be used for network normalization
		double maxTrafficIn = getMaxTrafficIn(currentComponent);
		if(currentComponent.isSpout()){
			//if the current component is a spout the regression is normalized with the max out traffic
			maxTrafficIn = MAX_TRAFFIC_SEC;
		}
		if(currentComponent.getName().equals("__acker")){
			maxTrafficIn = MAX_TRAFFIC_SEC;
		}
		if(maxTrafficIn==0) maxTrafficIn = 1;
		logger.debug("Max traffic (tuple/sec) in current component: "+maxTrafficIn+"; max cpu: "+MAX_CPU_LOAD);
		
		//print on a file a training set by reading the array with the load table
		for(int i=0; i<currentTrafficLoadArray.length; ++i){
			//splitting the array and getting traffic and load (each row is: traffic,cpu)
			String[] currentTrafficLoadArraySplitted = currentTrafficLoadArray[i].split(",");
			long traffic = Long.parseLong(currentTrafficLoadArraySplitted[0]);
			long load = Long.parseLong(currentTrafficLoadArraySplitted[1]);
			
			//normalizing traffic and load with the maxTrafficIn and the MAX_CPU_LOAD
			double normTraffic = (double)traffic / (double)maxTrafficIn;
			double normLoad = (double)load / (double)MAX_CPU_LOAD;
			
			//print the normalized value on training set file
			//out.println(currentTrafficLoadArray[i]); obsolete denormalized version
			out.println(normTraffic+","+normLoad);
			out.flush();
			logger.debug("\ttraffic=" + traffic + " (tuple/sec); load=" + load +
					"; normTraffic=" + normTraffic + "; normLoad=" + normLoad);
		}
		out.close();
		
		//creating the MLDataSet object from the training set file and start learning
		MLDataSet trainingSet = TrainingSetUtil.loadCSVTOMemory(CSVFormat.DECIMAL_POINT, trainFileName, true, 1, 1);
		Train train = new ResilientPropagation(network, trainingSet);
		int epoch = 1;
		int epochMax = 50000;
		logger.info("Start training neural network for " + currentComponent + "..");
		do {
			train.iteration();
			logger.debug("iter "+epoch+", error: "+train.getError());
			if(epoch % epochMax == 0) logger.debug("net_" + currentComponent + " trained with " + epoch + " epochs; error:" + train.getError() * 100 + "%");
			epoch++;
		} while (train.getError() > 0.00003 && epoch<=epochMax);
		train.finishTraining();
		logger.debug("net_" + currentComponent + " trained with " + epoch + " epochs.");
		
		//store the network on file
		EncogDirectoryPersistence.saveObject(networkFile, network);
		logger.info("Neural Network built for " + currentComponent + " (err=" + train.getError() * 100 + "%)\n");
		return network;
	}

	/**
	 * Initialize the ANN to infer cpu percentage usage of worker given the sum of 
	 * @throws FileNotFoundException
	 */
	private void initializeWorkerNetwork() throws FileNotFoundException{
		//create the neural network
		workerCpuUsageNetwork = new BasicNetwork();
		workerCpuUsageNetwork.addLayer(new BasicLayer(new ActivationTANH(), true, 3));
		workerCpuUsageNetwork.addLayer(new BasicLayer(new ActivationTANH(), true, 4));
		workerCpuUsageNetwork.addLayer(new BasicLayer(new ActivationTANH(), true, 1));
		workerCpuUsageNetwork.getStructure().finalizeStructure();
		workerCpuUsageNetwork.reset();
		
		//creating the neural network file and the training file with the related print writer
		File trainFile = new File(PATH+"dataset/train_workers.csv");
		File networkFile = new File(PATH+"neural_networks/net_workers.eg");
		PrintWriter out = new PrintWriter(trainFile);
		
		//get the maximum input traffic to normalize input and output traffic values
		Set<Component> components = graph.vertexSet();
		for(Component currentComponent : components){
			MAX_TRAFFIC_SEC_IN_WORKER += getMaxTrafficIn(currentComponent);
		}
		
		//print on a file a training set by reading the array with the load table
		for(int i=0; i<workerCpuUsageHistory.length; ++i){
			//splitting the array and getting traffic and load (each row is: traffic,cpu)
			String[] currentTripleInputOutputLoadArraySplitted = workerCpuUsageHistory[i].split(",");
			long inputTraffic = Long.parseLong(currentTripleInputOutputLoadArraySplitted[0]);
			long outputTraffic = Long.parseLong(currentTripleInputOutputLoadArraySplitted[1]);
			long load = Long.parseLong(currentTripleInputOutputLoadArraySplitted[2]);
			int workerCpuUsage = Integer.parseInt(currentTripleInputOutputLoadArraySplitted[3]);
			
			//normalizing traffic and load with the maxTrafficIn and the MAX_CPU_LOAD
			double normInputTraffic = (double)inputTraffic / MAX_TRAFFIC_SEC_IN_WORKER; //normalized on max traffic in a worker
			double normOutputTraffic = (double)outputTraffic / MAX_TRAFFIC_SEC_IN_WORKER; //idem
			double normLoad = (double)load / (double)MAX_CPU_LOAD; //normalized on max cpu load of a cluster
			double normWorkerCpuUsage = (double)workerCpuUsage / 100D; //normalized on 100, is a percentage
			
			//print the normalized value on training set file
			out.println(normInputTraffic+","+normOutputTraffic+","+normLoad+","+normWorkerCpuUsage);
			out.flush();
		}
		out.close();
		
		//creating the MLDataSet object from the training set file and start learning
		MLDataSet trainingSet = TrainingSetUtil.loadCSVTOMemory(CSVFormat.DECIMAL_POINT, PATH+"dataset/train_workers.csv", true, 3, 1);
		Train train = new ResilientPropagation(workerCpuUsageNetwork, trainingSet);
		int epoch = 1;
		int epochMax = 1000;
		logger.debug("Start training neural network for workers cpu usage..");
		do {
			train.iteration();
			logger.debug("iter "+epoch+", error: "+train.getError());
			if(epoch % epochMax == 0) logger.debug("net_workers trained with "+epoch+" epochs; error:"+train.getError()*100+"%");
			epoch++;
		} while (train.getError() > 0.015 && epoch<=epochMax);
		train.finishTraining();
		
		//store the network on file
		EncogDirectoryPersistence.saveObject(networkFile, workerCpuUsageNetwork);
		logger.info("Neural Network built for workers cpu usage\n");
		
	}
	
 	/**
 	 * Utility to compute the avg of an array (used for AlfaHistory, LoadHistory, TupleSizeHistory)
 	 * @param a the input array
 	 * @return the avg
 	 */
 	private static double avgCalculate(Double[] a){
		double sum = 0.0;
		for(int i=0; i<a.length; ++i){
			sum += a[i];
		}
		return (double)sum/(double)a.length;
	}

	/**
	 * cut the first element of the alfa table
	 * @param array the input array to cut
	 * @return a new cut array
	 */
	@SuppressWarnings("unused")
	private Double[] trimFirstAlfa(DefaultWeightedEdge edge){
		Double[] array = alfaTableHistory.get(edge);
		Double[] arrayTrimmed = new Double[array.length-1];
		for(int i=0; i<array.length-1; ++i){
			arrayTrimmed[i] = array[i];
		}
		return arrayTrimmed;
	}
	
	/**
	 * cut the first element of the alfa table
	 * @param array the input array to cut
	 * @return a new cut array
	 */
	@SuppressWarnings("unused")
	private String[] trimFirstLoad(Component component){
		String[] array = loadTableHistory.get(component);
		String[] arrayTrimmed = new String[array.length-1];
		for(int i=0; i<array.length-1; ++i){
			arrayTrimmed[i] = array[i];
		}
		return arrayTrimmed;
	}
 	
}
