package midlab.storm.autoscaling.topology;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

import midlab.storm.autoscaling.profiler.SelectivityProfiler;

import org.apache.log4j.Logger;
import org.jgrapht.graph.DefaultWeightedEdge;
import org.jgrapht.graph.ListenableDirectedWeightedGraph;

/**
 * Toology Builder
 * @author Federico Lombardi - Sapienza University of Rome
 *
 */
public class TopologyBuilder {

	private static final Logger logger = Logger.getLogger(TopologyBuilder.class);
	
	private static final String PROP_FILENAME = "topology.properties";
	
	/**
	 * Initialize a topology by reading a properties file named 'topology.properties'
	 * @return an object Topology
	 * @throws Exception 
	 */
	public static Topology initializeTopology() throws Exception{
		
		//loading properties file
		//String propFileName = "topology.properties";
		Properties prop = new Properties();
		InputStream inputStream = SelectivityProfiler.class.getClassLoader().getResourceAsStream(PROP_FILENAME);
		try {
			prop.load(inputStream);
		} catch (IOException e) {
			logger.error("Some problem occur while reading property file '" + PROP_FILENAME + "'. It might be not found in the classpath");	
			e.printStackTrace();
		}
		
		//Graph and List of Spouts and Bolts
		ListenableDirectedWeightedGraph<Component,DefaultWeightedEdge> g = new ListenableDirectedWeightedGraph<Component,DefaultWeightedEdge>(DefaultWeightedEdge.class);
		ListenableDirectedWeightedGraph<String,DefaultWeightedEdge> gGui = new ListenableDirectedWeightedGraph<String,DefaultWeightedEdge>(DefaultWeightedEdge.class);
		ArrayList<Spout> spouts = new ArrayList<Spout>();
		ArrayList<Bolt> bolts = new ArrayList<Bolt>();
		
		String[] componentParallelism;
		String nameComponent;
		int parallelism;
		
		logger.info("Loading components..");
		
		//Spouts
		int i = 0;
		while(prop.containsKey("spout"+i)){
			componentParallelism = prop.getProperty("spout"+i).split(",");
			nameComponent = componentParallelism[0];
			parallelism = Integer.parseInt(componentParallelism[1]);
			//spouts.add(i, new Spout(prop.getProperty("spout"+i)));
			spouts.add(i, new Spout(nameComponent, parallelism));
			logger.info("\tadded spout: " + spouts.get(i).toString());
			++i;
		}
		
		//Bolts
		i = 0;
		while(prop.containsKey("bolt"+i)){
			componentParallelism = prop.getProperty("bolt"+i).split(",");
			nameComponent = componentParallelism[0];
			parallelism = Integer.parseInt(componentParallelism[1]);
			//bolts.add(i, new Bolt(prop.getProperty("bolt"+i)));
			bolts.add(i, new Bolt(nameComponent, parallelism));
			logger.info("\tadded bolt:  " + bolts.get(i).toString());
			++i;
		}
		
		//task
		int indexSource = 0;
		int indexDest = 0;
		boolean foundSource = false;
		boolean isSpout = false;
		boolean foundDest = false;
		int index = -1;
		String componentName = "";
		Component component;
		String[] idTask;
		Iterator<Bolt> itBolt;
		Iterator<Spout> itSpout;
		Bolt currentBolt = null;
		Spout currentSpout = null;
		int indexMin = 0;
		int indexMax = 0;
		int dimTaskArray = 0;
		i=0;
		logger.info("Loading Tasks..");
		while(prop.containsKey("task"+i)){
			String task = prop.getProperty("task"+i);
			String[] tasks = task.split(",");
			componentName = tasks[0].replaceAll(" ", "");
			idTask = tasks[1].replaceAll(" ", "").split("-");
			logger.info("Tasks for component "+componentName+": ");
			for(int j=0; j<idTask.length; ++j){
				logger.info("\t"+idTask[j]+" ");
			}
			
			//checking if the referred component is a bolt, searching its index in the ArrayList
			itBolt = bolts.iterator();
			while(itBolt.hasNext()){
				currentBolt = itBolt.next();
				if(currentBolt.toString().equals(componentName)){
					foundSource = true;
					index = bolts.indexOf(currentBolt);
					break;
				}
			}
			
			//if it is not a bolt, it would be a spout. Searching its index in the ArrayList
			if(!foundSource){
				itSpout = spouts.iterator();
				while(itSpout.hasNext()){
					currentSpout = itSpout.next();
					if(currentSpout.toString().equals(componentName)){
						foundSource = true;
						isSpout = true;
						index = spouts.indexOf(currentSpout);
						break;
					}
				}
			}
			
			//if it is not found a bolt neither a spout, exit with error
			if(!foundSource){
				logger.error("Impossible to find the component "+componentName+". Exiting..");
				System.exit(1);
			}
			
			//setting the component to the spout or bolt found
			if(!isSpout){
				component = bolts.get(index);
			}
			else{
				component = spouts.get(index);
			}
			
			//adding all the tasks to the component			
			if(idTask.length == 1){
				component.addTask(Integer.parseInt(idTask[0]));
				logger.info("Associated task="+idTask[0]+" to component "+component.toString());
			}
			else{
				if(idTask.length == 2){
					indexMin = Integer.parseInt(idTask[0]);
					indexMax = Integer.parseInt(idTask[1])+1;
					dimTaskArray = indexMax - indexMin;
					idTask = new String[dimTaskArray];
					for(int j=0; j<dimTaskArray; ++j){
						idTask[j] = ""+indexMin;
						++indexMin;
					}
					for(int j=0; j<idTask.length; ++j){
						component.addTask(Integer.parseInt(idTask[j]));
						logger.info("Associated task="+idTask[j]+" to component "+component.toString());
					}
				}
				else{
					logger.error("Bad formulation of tasks in property file. Exiting..");
					System.exit(1);
				}
			}			
			
			//reset of the variable
			++i;
			foundSource = false;
			isSpout = false;
			index = -1;
			
		}
	
		
		//add vertexes to graph
		logger.info("Loading vertexes (components) to the graph..");
		itBolt = bolts.iterator();
		while(itBolt.hasNext()){
			component = itBolt.next();
			g.addVertex(component);
			gGui.addVertex(component.toString());
		}
		itSpout = spouts.iterator();
		while(itSpout.hasNext()){
			component = itSpout.next();
			g.addVertex(component);
			gGui.addVertex(component.toString());
		}
		
		//reset variables for adding edges
		i = 0;
		indexSource = 0;
		indexDest = 0;
		foundSource = false;
		isSpout = false;
		foundDest = false;	
		
		//Edges iterations
		logger.info("Loading edges..");
		while(prop.containsKey("edge"+i)){
			String edge = prop.getProperty("edge"+i);
			String[] vertexes = edge.split(",");
			logger.info("	Setting edge: " + vertexes[0] + "-" + vertexes[1]);
			vertexes[0] = vertexes[0].replaceAll(" ", "");
			vertexes[1] = vertexes[1].replaceAll(" ", "");
			
			//searching a source vertex with a bolt
			itBolt = bolts.iterator();
			while(itBolt.hasNext()){
				currentBolt = itBolt.next();
				if(currentBolt.getName().equals(vertexes[0])){
					foundSource = true;
					break;
				}
			}
			
			//if it is found a source index with a bolt, setting the sourceIndex
			if(foundSource){
				indexSource = bolts.indexOf(currentBolt);
			}
			
			//if it is not found a source index with bolt, trying to find with a spout
			else{
				itSpout = spouts.iterator();
				while(itSpout.hasNext()){
					currentSpout = itSpout.next();
					if(currentSpout.getName().equals(vertexes[0])){
						foundSource = true;
						isSpout = true;
						break;
					}
				}
				if(foundSource){
					indexSource = spouts.indexOf(currentSpout);
				}
				else{
					logger.error("Unable to find any source index. Exiting..");
					System.exit(1);
				}
			}
						
			//here we have the source vertex. Searching for a bolt index in the dest vertexes
			itBolt = bolts.iterator();
			while(itBolt.hasNext()){
				currentBolt = itBolt.next();
				if(currentBolt.toString().equals(vertexes[1])){
					foundDest = true;
					break;
				}
			}
			
			if(foundDest){
				indexDest = bolts.indexOf(currentBolt);
				if(isSpout){
					g.addEdge(spouts.get(indexSource), bolts.get(indexDest));
					gGui.addEdge(spouts.get(indexSource).toString(), bolts.get(indexDest).toString());
					logger.info("\tadded edge:  " + spouts.get(indexSource).toString() + "-" + bolts.get(indexDest).toString());
				}
				else{
					g.addEdge(bolts.get(indexSource), bolts.get(indexDest));
					gGui.addEdge(bolts.get(indexSource).toString(), bolts.get(indexDest).toString());
					logger.info("\tadded edge:  " + bolts.get(indexSource).toString() + "-" + bolts.get(indexDest).toString());
				}
			}
			else{
				logger.error("Unable to find any dest index. Exiting");
				System.exit(1);
			}
			
			//reset variables
			foundSource = false;
			foundDest = false;
			isSpout = false;
			++i;
		}
		
		Topology topology = new Topology(prop.getProperty("name"), g, gGui);
		logger.info("Topology built successfully.");
		return topology;
	}
	
	/**
	 * 
	 * @param t
	 * @param dataDirectory
	 * @return
	 * @throws Exception
	 */
	public static Topology updateTopologyTaskList(Topology t, String dataDirectory) throws Exception{
		
		String propFileName = "topology.properties";
		File dataDirFile = new File(dataDirectory);
		File propFile = new File(dataDirFile, propFileName);
		Properties prop = new Properties();
		prop.load(new FileReader(propFile));
		
		//task
		ListenableDirectedWeightedGraph<Component,DefaultWeightedEdge> newGraph = new ListenableDirectedWeightedGraph<Component,DefaultWeightedEdge>(DefaultWeightedEdge.class);
		Set<Spout> setSpout = t.getSpouts();
		Set<Bolt> setBolt = t.getBolts();
		
		ArrayList<Spout> spouts = new ArrayList<Spout>();
		ArrayList<Bolt> bolts = new ArrayList<Bolt>();
		spouts.addAll(setSpout);
		bolts.addAll(setBolt);
		
		int indexSource = 0;
		int indexDest = 0;
		boolean foundSource = false;
		boolean isSpout = false;
		boolean foundDest = false;
		int index = -1;
		String componentName = "";
		Component component;
		String[] idTask;
		Iterator<Bolt> itBolt;
		Iterator<Spout> itSpout;
		Bolt currentBolt = null;
		Spout currentSpout = null;
		int indexMin = 0;
		int indexMax = 0;
		int dimTaskArray = 0;
		int i=0;
		
				
		//adding the task list with the new one in the properties file
		logger.info("Loading tasks..");
		while(prop.containsKey("task"+i)){
			String task = prop.getProperty("task"+i);
			String[] tasks = task.split(",");
			componentName = tasks[0].replaceAll(" ", "");
			idTask = tasks[1].replaceAll(" ", "").split("-");
			logger.info("Tasks for component "+componentName+": ");
			for(int j=0; j<idTask.length; ++j){
				logger.info("\t"+idTask[j]+" ");
			}
			
			//checking if the referred component is a bolt, searching its index in the ArrayList
			itBolt = bolts.iterator();
			while(itBolt.hasNext()){
				currentBolt = itBolt.next();
				if(currentBolt.toString().equals(componentName)){
					foundSource = true;
					index = bolts.indexOf(currentBolt);
					break;
				}
			}
			
			//if it is not a bolt, it would be a spout. Searching its index in the ArrayList
			if(!foundSource){
				itSpout = spouts.iterator();
				while(itSpout.hasNext()){
					currentSpout = itSpout.next();
					if(currentSpout.toString().equals(componentName)){
						foundSource = true;
						isSpout = true;
						index = spouts.indexOf(currentSpout);
						break;
					}
				}
			}
			
			//if it is not found a bolt neither a spout, exit with error
			if(!foundSource){
				logger.error("Impossible to find the component "+componentName+". Exiting..");
				System.exit(1);
			}
			
			//setting the component to the spout or bolt found
			if(!isSpout){
				component = bolts.get(index);
			}
			else{
				component = spouts.get(index);
			}
			
			//deleting all the task for the component
			component.deleteAllTask();
			
			//adding all the tasks to the component			
			if(idTask.length == 1){
				component.addTask(Integer.parseInt(idTask[0]));
				logger.info("Associated taskId="+idTask[0]+" to component "+component.toString());
			}
			else{
				if(idTask.length == 2){
					indexMin = Integer.parseInt(idTask[0]);
					indexMax = Integer.parseInt(idTask[1])+1;
					dimTaskArray = indexMax - indexMin;
					idTask = new String[dimTaskArray];
					for(int j=0; j<dimTaskArray; ++j){
						idTask[j] = ""+indexMin;
						++indexMin;
					}
					for(int j=0; j<idTask.length; ++j){
						component.addTask(Integer.parseInt(idTask[j]));
						logger.info("Associated taskId="+idTask[j]+" to component "+component.toString());
					}
				}
				else{
					System.err.println("Bad formulation of tasks in property file. Exiting..");
					System.exit(1);
				}
			}			
			
			//reset of the variable
			++i;
			foundSource = false;
			isSpout = false;
			index = -1;
			
		}
		
		//add vertexes to graph
				itBolt = bolts.iterator();
				while(itBolt.hasNext()){
					component = itBolt.next();
					newGraph.addVertex(component);
					//gGui.addVertex(component.toString());
				}
				itSpout = spouts.iterator();
				while(itSpout.hasNext()){
					component = itSpout.next();
					newGraph.addVertex(component);
					//gGui.addVertex(component.toString());
				}
				
				//reset variables for adding edges
				i = 0;
				indexSource = 0;
				indexDest = 0;
				foundSource = false;
				isSpout = false;
				foundDest = false;	
				
				//Edges iterations
				logger.info("Loading edges..");
				while(prop.containsKey("edge"+i)){
					String edge = prop.getProperty("edge"+i);
					String[] vertexes = edge.split(",");
					vertexes[0] = vertexes[0].replaceAll(" ", "");
					vertexes[1] = vertexes[1].replaceAll(" ", "");
					
					//searching a source vertex with a bolt
					itBolt = bolts.iterator();
					while(itBolt.hasNext()){
						currentBolt = itBolt.next();
						if(currentBolt.getName().equals(vertexes[0])){
							foundSource = true;
							break;
						}
					}
					
					//if it is found a source index with a bolt, setting the sourceIndex
					if(foundSource){
						indexSource = bolts.indexOf(currentBolt);
					}
					
					//if it is not found a source index with bolt, trying to find with a spout
					else{
						itSpout = spouts.iterator();
						while(itSpout.hasNext()){
							currentSpout = itSpout.next();
							if(currentSpout.getName().equals(vertexes[0])){
								foundSource = true;
								isSpout = true;
								break;
							}
						}
						if(foundSource){
							indexSource = spouts.indexOf(currentSpout);
						}
						else{
							logger.error("Unable to find any source index. Exiting..");
							System.exit(1);
						}
					}
								
					//here we have the source vertex. Searching for a bolt index in the dest vertexes
					itBolt = bolts.iterator();
					while(itBolt.hasNext()){
						currentBolt = itBolt.next();
						if(currentBolt.toString().equals(vertexes[1])){
							foundDest = true;
							break;
						}
					}
					
					if(foundDest){
						indexDest = bolts.indexOf(currentBolt);
						if(isSpout){
							newGraph.addEdge(spouts.get(indexSource), bolts.get(indexDest));
							//gGui.addEdge(spouts.get(indexSource).toString(), bolts.get(indexDest).toString());
							logger.info("\tadded edge:  " + spouts.get(indexSource).toString() + "-" + bolts.get(indexDest).toString());
						}
						else{
							newGraph.addEdge(bolts.get(indexSource), bolts.get(indexDest));
							//gGui.addEdge(bolts.get(indexSource).toString(), bolts.get(indexDest).toString());
							System.out.println("\tadded edge:  " + bolts.get(indexSource).toString() + "-" + bolts.get(indexDest).toString());
						}
					}
					else{
						System.err.println("Unable to find any dest index. Exiting");
						System.exit(1);
					}
					
					//reset variables
					foundSource = false;
					foundDest = false;
					isSpout = false;
					++i;
				}
		
		t.updateGraphInTopology(newGraph);
		
		return t;
		
	}
	
}
