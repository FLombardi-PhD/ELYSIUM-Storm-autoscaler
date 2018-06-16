package midlab.storm.scheduler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import midlab.storm.scheduler.data.Allocation;
import midlab.storm.scheduler.data.Executor;
import midlab.storm.scheduler.data.Node;
import midlab.storm.scheduler.data.StormCluster;
import midlab.storm.scheduler.data.Worker;
import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.SupervisorDetails;
import backtype.storm.scheduler.WorkerSlot;

public class SchedulerUtils {
	
	private static Logger logger = Logger.getLogger(SchedulerUtils.class);
	
	public static void applyAllocation(Cluster cluster, StormCluster stormCluster, Allocation allocation) {
		
		// free all available slots
		for (SupervisorDetails supervisor : cluster.getSupervisors().values()) {
			Set<Integer> usedPorts = cluster.getUsedPorts(supervisor);
			for (int usedPort : usedPorts)
				cluster.freeSlot(new WorkerSlot(supervisor.getId(), usedPort));
		}
		
		// prepare a data structure to easily retrieve a WorkerSlot (storm code) given a Worker (midlab code)
		Map<Worker, WorkerSlot> workerSlotMap = new HashMap<Worker, WorkerSlot>();
		for (Node node : stormCluster.getNodes()) {
			SupervisorDetails supervisor = cluster.getSupervisorsByHost(node.getHostname()).get(0);
			for (WorkerSlot workerSlot : cluster.getAssignableSlots(supervisor))
				workerSlotMap.put(node.getWorker(workerSlot.getPort()), workerSlot);
		}
		
		// apply best allocation
		for (Worker worker : allocation.getAllocation().keySet()) {
			List<Executor> executorList = allocation.getAllocation().get(worker);
			if (!executorList.isEmpty()) {
				// get topo id
				String topologyId = executorList.get(0).getTopology().getId();
				
				// prepare list of executors (storm code)
				Collection<ExecutorDetails> executors = new ArrayList<ExecutorDetails>();
				for (Executor executor : executorList)
					executors.add(executor.getStormExecutor());
				
				// and finally assign
				logger.info("Going to assign " + executors.size() + " executors of topology " + topologyId + " to slot " + workerSlotMap.get(worker));
				cluster.assign(workerSlotMap.get(worker), topologyId, executors);
			}
		}
		
	}
	
}
