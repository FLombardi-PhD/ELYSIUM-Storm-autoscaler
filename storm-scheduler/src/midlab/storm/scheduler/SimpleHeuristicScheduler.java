package midlab.storm.scheduler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import midlab.storm.scheduler.data.Allocation;
import midlab.storm.scheduler.data.Executor;
import midlab.storm.scheduler.data.Node;
import midlab.storm.scheduler.data.StormCluster;
import midlab.storm.scheduler.data.Topology;
import midlab.storm.scheduler.data.Worker;

public class SimpleHeuristicScheduler implements MidlabScheduler {
	
	private Logger logger = Logger.getLogger(SimpleHeuristicScheduler.class);

	@Override
	public Allocation schedule(StormCluster stormCluster, Map<String, Topology> topologyMap) {
		Allocation allocation = new Allocation(stormCluster);
		
		// prepare the list of all available workers
		List<Worker> allWorkerList = new ArrayList<Worker>();
		for (Node node : stormCluster.getNodes())
			for (Worker worker : node.getWorkers())
				allWorkerList.add(worker);
		
		for (Topology topology : topologyMap.values()) {
			logger.debug("Schedule topology: " + topology);
			List<Executor> executorList = new ArrayList<Executor>(topology.getExecutors());
			Collections.sort(executorList); // ascending sort
			logger.debug("Sorted executor list: " + executorList);
			
			// worker set to comply with the constraint on the number of workers to use
			List<Worker> usedWorkerList = new ArrayList<Worker>();
			List<Worker> usableWorkerList = allWorkerList;
			
			for (int i = executorList.size() - 1; i >= 0; i--) {
				Executor executor = executorList.get(i);
				double tmpBestWeight = -1;
				Worker tmpBestWorker = null;
				logger.debug("Allocating " + executor);
				
				if (usedWorkerList.size() == topology.getWorkerCount())
					usableWorkerList = usedWorkerList;
				logger.debug("Usable worker list: " + usableWorkerList);
				
				// iterate through all usable workers
				for (Worker worker : usableWorkerList) {
					// try to allocate executor to this worker and compute allocation weight
					allocation.allocate(worker, executor);
					allocation.computeWeight();
					logger.debug("Allocation weight if " + executor + " is allocated to " + worker + ": " + allocation.getWeight() + " (" + allocation.getMeaningfulnessMsg() + ")");
					if (tmpBestWorker == null || (allocation.isWeightMeaningful() && tmpBestWeight > allocation.getWeight())) {
						tmpBestWeight = allocation.getWeight();
						tmpBestWorker = worker;
					}
					allocation.deallocate(executor);
				}
				
				// allocate to the best worker
				allocation.allocate(tmpBestWorker, executor);
				logger.debug(executor.toString() + " allocated to " + tmpBestWorker);
				
				if (!usedWorkerList.contains(tmpBestWorker))
					usedWorkerList.add(tmpBestWorker);
			}
			
			// remove the workers used by this topology so as to avoid that a worker is used by more topologies
			allWorkerList.removeAll(usedWorkerList);
			
		}
		return allocation;
	}

}
