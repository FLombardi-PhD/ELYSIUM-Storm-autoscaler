package midlab.storm.autoscaling.profiler;

import java.io.PrintWriter;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Set;

import org.apache.log4j.Logger;
import org.jgrapht.graph.DefaultWeightedEdge;
import org.jgrapht.graph.ListenableDirectedWeightedGraph;

import midlab.storm.autoscaling.topology.Component;
import midlab.storm.autoscaling.topology.Topology;
import midlab.storm.autoscaling.utility.GraphUtils;
import midlab.storm.autoscaling.utility.Utils;

/**
 * Profiler of selectivity alfa
 * @author Federico Lombardi - Sapienza University of Rome
 *
 */
public class SelectivityProfiler {

	private static final Logger logger = Logger.getLogger(SelectivityProfiler.class);
	
	/**
	 * Extend the array given as input and return the same array with the first entry void
	 * @param a the input array
	 * @return the extended array
	 */
	public static Double[] extendAlfaArray(Double[] a){
		if (a != null) {
			Double[] b = new Double[a.length+1];
			for (int i=a.length-1; i>=0; --i) {
				b[i+1] = a[i];
			}
			return b;
		}
		else {
			Double[] b = new Double[1];
			return b;
		}
	}
		
	/**
	 * 
	 * @param t
	 * @param currentComponent
	 * @param iter
	 * @param outAlfa
	 * @return
	 */
	public static Topology profileSelectivity(Topology t, Component currentComponent, int iter, PrintWriter outAlfa){
		ListenableDirectedWeightedGraph<Component,DefaultWeightedEdge> g = t.getGraph();
		long totStreamIn = GraphUtils.getTotalStreamIn(g, currentComponent);
		Set<DefaultWeightedEdge> outEdges = g.outgoingEdgesOf(currentComponent);
		Iterator<DefaultWeightedEdge> itOutEdge = outEdges.iterator();
		while(itOutEdge.hasNext()){
						
			// getting the current dege and the destination node
			DefaultWeightedEdge currentEdge = itOutEdge.next();
			Component dest = g.getEdgeTarget(currentEdge);
			
			// calculating the total output stream and alfa
			long totStreamOut = (long)g.getEdgeWeight(currentEdge);
			
			// controller to avoid NaN while computing alfa
			if(totStreamIn == 0)
				totStreamIn = 1;
						
			// cheking if the source node is a spout
			if(currentComponent.isSpout() && totStreamOut != 0)
				totStreamIn = totStreamOut;
			
			// compute alfa
			double alfa = (double)totStreamOut / (double)totStreamIn;
			
			//replacing the old hashtable with the new one, by adding the new alfa value 
			Hashtable<DefaultWeightedEdge,Double[]> alfaTableHistory = t.getAlfaTableHistory();
			Double[] alfaArray = extendAlfaArray(t.getAlfaTableHistory().get(currentEdge));
			alfaArray[0] = alfa;
			
			if (iter>0) {
				alfaTableHistory.put(currentEdge, alfaArray);
				t.setAlfaTableHistory(alfaTableHistory);
				logger.info("\tdest component: "+dest.toString()+", stream out = "+totStreamOut+", alfa = "+alfa);
				logger.info("\tedge: "+currentEdge.toString()+", alfa avg: "+Utils.avgCalculate(alfaArray)+", alfa history: "+Utils.printArray(alfaArray));
				outAlfa.println(currentEdge.toString()+","+alfa);
				outAlfa.flush();
			}
		}
		System.out.println();
		return t;
	}
	
}
