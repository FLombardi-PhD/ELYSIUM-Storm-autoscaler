package midlab.storm.autoscaling.profiler;

import java.io.PrintWriter;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Set;

import midlab.storm.autoscaling.topology.Component;
import midlab.storm.autoscaling.topology.Topology;
import midlab.storm.autoscaling.utility.GraphUtils;
import midlab.storm.autoscaling.utility.Utils;

import org.jgrapht.graph.DefaultWeightedEdge;
import org.jgrapht.graph.ListenableDirectedWeightedGraph;

/**
 * Profiler of tuple size
 * @author Federico Lombardi - Sapienza University of Rome
 *
 */
public class TupleSizeProfiler {

	/**
	 * Extend the array given as input and return the same array with the first entry void
	 * @param a the input array
	 * @return the extended array
	 */
	public static Double[] extendTupleSizeArray(Double[] a){
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
	 * @throws Exception 
	 */
	public static Topology profileTupleSize(Topology t, Component currentComponent, int iter, PrintWriter outTupleSize) throws Exception{
		ListenableDirectedWeightedGraph<Component,DefaultWeightedEdge> g = t.getGraph();
		Set<DefaultWeightedEdge> outEdges = g.outgoingEdgesOf(currentComponent);
		Iterator<DefaultWeightedEdge> itOutEdge = outEdges.iterator();
		while (itOutEdge.hasNext()) {
			//getting the current dege and the destination node
			DefaultWeightedEdge currentEdge = itOutEdge.next();
			Component dest = g.getEdgeTarget(currentEdge);
			double currentAvgTupleSize = GraphUtils.getTupleSize(g, currentComponent, dest);
			
			//replacing the old hashtable with the new one, by adding the new alfa value 
			Hashtable<DefaultWeightedEdge,Double[]> tupleSizeTableHistory = t.getTupleSizeTableHistory();
			Double[] tupleSizeArray = extendTupleSizeArray(t.getTupleSizeTableHistory().get(currentEdge));
			tupleSizeArray[0] = currentAvgTupleSize;
			if (iter>0) {
				tupleSizeTableHistory.put(currentEdge, tupleSizeArray);
				t.setTupleSizeTableHistory(tupleSizeTableHistory);
				System.out.println("\tcurrent tuple size = "+currentAvgTupleSize+", tuple size avg: "+Utils.avgCalculate(tupleSizeArray)+", tuple size history: "+Utils.printArray(tupleSizeArray));
				outTupleSize.println(currentEdge.toString()+","+currentAvgTupleSize);
				outTupleSize.flush();
			}
		}
		return t;
	}
	
}
