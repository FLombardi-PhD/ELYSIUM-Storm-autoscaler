package midlab.storm.autoscaling;

import java.util.Hashtable;
import java.util.Iterator;
import java.util.Set;

import org.encog.ml.data.MLData;
import org.encog.ml.data.basic.BasicMLData;
import org.encog.neural.networks.BasicNetwork;

import midlab.storm.autoscaling.forecasting.Forecaster;
import midlab.storm.autoscaling.topology.Component;
import midlab.storm.autoscaling.topology.Topology;
import midlab.storm.autoscaling.utility.GraphUtils;
import midlab.storm.scheduler.data.Allocation;
import midlab.storm.scheduler.data.StormCluster;

public class AutoScaler_ {

	public AutoScaler_(){
		
	}
	
	public static void main(String[] args) throws Exception{
		
		Forecaster f = new Forecaster();
		
		System.out.println("SIZE: "+f.getTopology().getGraph().toString());
		
		while(true){
			
			/* 
			 * 1) calcolo il grafo dei traffici
			 * 2) stimo con le reti neurali dei component, i carichi di cpu degli executor dividendo
			 * 	  il traffico in ingresso in un component (livello logico) per il grado di parlallelismo
			 * 	  di quel component cosi da avere per il traffico in ingresso per executor
			 * 3) per ogni configurazione con 'i' macchine for(i=1 to n) :
			 * 		- prendo un istanza di Allocation da cui prendo una Map<Executor, Worker>
			 * 		- sommo i contributi di carico di cpu degli executor per ogni macchina (worker)
			 * 		-
			 */
			
			// 1) calcolo il grafo dei traffici
			//f.computeForecastedTrafficGraph();
			
			// 2) calcolo per ogni component il consumo di cpu a livello di executor
			//f.computeExpectedLoadTable();
			
			f.computeForecastedTrafficGraphAndLoadTable();
			
			// 3) for i=1 to n verifico l'allocazione
			StormCluster stormCluster = new StormCluster(null);
			f.isMinimumAllocation(stormCluster, new Allocation(stormCluster));
			
			
			Thread.sleep(5000);
			//f.getTopology().updateGraphInTopology(GraphUtils.updateWeight(f.getTopology().getGraph()));
		}
		
		
	}
	
	
	public long shiftTimestamp(int secondFromStart){
		long timestamp = System.currentTimeMillis();
		return timestamp;
	}
}
