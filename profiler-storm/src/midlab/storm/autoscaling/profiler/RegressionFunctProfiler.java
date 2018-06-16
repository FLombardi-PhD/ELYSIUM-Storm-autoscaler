package midlab.storm.autoscaling.profiler;

import java.io.PrintWriter;
import java.util.Hashtable;

import midlab.storm.autoscaling.topology.Component;
import midlab.storm.autoscaling.topology.Topology;
import midlab.storm.autoscaling.utility.GraphUtils;

import org.apache.log4j.Logger;
import org.jgrapht.graph.DefaultWeightedEdge;
import org.jgrapht.graph.ListenableDirectedWeightedGraph;

/**
 * Profiler of regression function
 * @author Federico Lombardi - Sapienza University of Rome
 *
 */
public class RegressionFunctProfiler {

	private static final Logger logger = Logger.getLogger(RegressionFunctProfiler.class);
	
	/**
	 * Extend the array given as input and return the same array with the first entry void
	 * @param a the input array
	 * @return the extended array
	 */
	public static String[] extendLoadArray(String[] a){
		if(a != null){
			String[] b = new String[a.length+1];
			for(int i=a.length-1; i>=0; --i){
				b[i+1] = a[i];
			}
			return b;
		}
		else{
			String[] b = new String[1];
			return b;
		}
	}
	
	/**
	 * Update the loadTableHistory by adding the new profiled value of 'Component, [tot_stream_in_component,load , ... ]'
	 * @param t the topology
	 * @param currentComponent 
	 * @param iter
	 * @param totStreamInOrOut the input stream if the currentComponent is a bolt, the output stream otherwise (spout)
	 * @param outLoad
	 * @return
	 * @throws Exception 
	 */
	public static Topology profileRegression(Topology t, Component currentComponent, int iter, PrintWriter outLoad) throws Exception{
		//get the graph of topology
		ListenableDirectedWeightedGraph<Component,DefaultWeightedEdge> g = t.getGraph();
		
		//get the total input stream (case component=bolt) or outpput stream (case component=spout)
		long totStreamInOrOut = 0;
		if(currentComponent.isBolt()) totStreamInOrOut = GraphUtils.getTotalStreamIn(g, currentComponent);
		else totStreamInOrOut = GraphUtils.getTotalStreamOut(g, currentComponent);
		
		//get the total load of the current component
		long totLoad = GraphUtils.getTotalLoad(g, currentComponent);
		
		//get the hastable containing <Component, 
		Hashtable<Component,String[]> loadTable = t.getLoadTableHistory();
		
		//extend the array containing the history of the couple <stream_in, load>
		String[] loadArray = extendLoadArray(t.getLoadTableHistory().get(currentComponent));
		
		//INSERIRE QUI LA VARIANTE: per ogni executor (task_id) prendi il suo traffico in ingresso:
		//select sum(TRAFFIC) from APP.TRAFFIC where DESTINATION_TASK_ID=task
		//e accoppialo con il suo carico
		//select LOAD from APP.LOAD where TASK_ID=task
		int cont = 0;
		String coupleTrafficLoad = "";
		logger.info("Current component: "+currentComponent.toString());
		for (int task : currentComponent.getTasks()) {
			if (currentComponent.isBolt())
				coupleTrafficLoad = ""+GraphUtils.getStreamInTask(task)+","+GraphUtils.getLoadOfTask(task);
			
			if (currentComponent.isSpout())
				coupleTrafficLoad = ""+GraphUtils.getStreamOutTask(task)+","+GraphUtils.getLoadOfTask(task);
			
			loadArray[0] = coupleTrafficLoad;
			
			if (currentComponent.isBolt())
				logger.info("\ttask"+task+": load: "+coupleTrafficLoad.split(",")[1]+"; input traffic: "+coupleTrafficLoad.split(",")[0]);
			
			if (currentComponent.isSpout())
				logger.info("\ttask"+task+": load: "+coupleTrafficLoad.split(",")[1]+"; output traffic: "+coupleTrafficLoad.split(",")[0]);
			
			outLoad.println(currentComponent.toString()+","+coupleTrafficLoad);
			outLoad.flush();
			++cont;
			
			if (cont<currentComponent.getTasks().size())
				loadArray = extendLoadArray(loadArray);
		}
		
		
		//put as first value of the load array the current couple <Component, [tot_stream_in_component,load , ... ]>
		//loadArray[0] = ""+totStreamInOrOut+","+totLoad;
		if (iter>0) {
			//update the loadTable
			loadTable.put(currentComponent, loadArray);
			t.setLoadTableHistory(loadTable);
			//lo faccio nel ciclo sopra outLoad.println(currentComponent.toString()+","+totStreamInOrOut+","+totLoad);
			//outLoad.flush();
			logger.info("\taggregated resources:");
			logger.info("\t    tot load: "+totLoad);
			
			if (currentComponent.isBolt())
				logger.info("\t    tot stream in: "+totStreamInOrOut);
			
			if (currentComponent.isSpout())
				logger.info("\t    tot stream out: "+totStreamInOrOut);
		}
		System.out.println();
		return t;
	}
	
}
