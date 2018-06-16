package midlab.storm.scheduler.data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.log4j.Logger;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.SchedulerAssignment;
import backtype.storm.scheduler.WorkerSlot;

import midlab.storm.scheduler.SchedulerConf;

public class Allocation {
	
	private final StormCluster stormCluster;
	
	/**
	 * worker -> executors allocated to that worker
	 */
	private Map<Worker, List<Executor>> allocation;
	
	/**
	 * executor -> worker where that executor is allocated
	 */
	private Map<Executor, Worker> allocatedExecutorMap;
	
	/**
	 * woker -> topology whose executors are allocated to that worker
	 * this is to enforce the constraint that a worker has to contain executors of a single topology
	 */
	private Map<Worker, Topology> topoToWorkerConstraint;
	
	/**
	 * topology -> number of workers used by that topology
	 * this is to enforce the constraint on the number of workers that a topology has to use
	 */
	private Map<Topology, Integer> workerCountConstraint;
	
	private double weight;
	private double meanLoad;
	private double loadStdev;
	private long totalTraffic;
	private long interNodeTraffic;
	private long interSlotTraffic;
	private String toString;
	
	private Logger logger = Logger.getLogger(Allocation.class);
	
	public Allocation(StormCluster stormCluster) {
		this.stormCluster = stormCluster;
		allocation = new HashMap<Worker, List<Executor>>();
		allocatedExecutorMap = new HashMap<Executor, Worker>();
		topoToWorkerConstraint = new HashMap<Worker, Topology>();
		workerCountConstraint = new HashMap<Topology, Integer>();
	}
	
	public Allocation(StormCluster stormCluster, Cluster cluster, Map<String, Topology> topologyMap) {
		this(stormCluster);
		for (Topology topology : topologyMap.values()) {
			SchedulerAssignment assignment = cluster.getAssignmentById(topology.getId());
			if (assignment != null) {
				for (ExecutorDetails ed : assignment.getExecutors()) {
					WorkerSlot workerSlot = assignment.getExecutorToSlot().get(ed);
					Node node = stormCluster.getNodeById(workerSlot.getNodeId());
					Worker worker = node.getWorker(workerSlot.getPort());
					Executor executor = topology.getExecutorByTaskId(ed.getStartTask());
					allocate(worker, executor);
				}
			}
		}
	}
	
	public Map<Worker, List<Executor>> getAllocation() {
		return allocation;
	}
	
	public boolean isAllocated(Executor executor) {
		return allocatedExecutorMap.get(executor) != null;
	}
	
	public int getUsedWorkerCount() {
		return allocation.keySet().size();
	}
	
	public List<Worker> getAvailableWorkers() {
		List<Worker> availableWorkers = new ArrayList<Worker>();
		for (Node node : stormCluster.getNodes())
			for (Worker worker : node.getWorkers())
				if (isAvailable(worker))
					availableWorkers.add(worker);
		return availableWorkers;
	}
	
	public List<Worker> getAvailableWorkers(Node node) {
		List<Worker> availableWorkers = new ArrayList<Worker>();
		for (Worker worker : node.getWorkers())
			if (isAvailable(worker))
				availableWorkers.add(worker);
		return availableWorkers;
	}
	
	public int getAvailableWorkerCount(Node node) {
		int count = 0;
		for (Worker worker : node.getWorkers())
			if (isAvailable(worker))
				count++;
		return count;
	}
	
	public boolean isAvailable(Worker worker) {
		return allocation.get(worker) == null;
	}

	public void allocate(Worker worker, Executor executor) {
		toString = null;
		Worker currentAllocation = allocatedExecutorMap.get(executor); 
		if (currentAllocation != null)
			throw new RuntimeException("Executor " + executor + " already allocated to worker " + currentAllocation);
		List<Executor> executorList = allocation.get(worker);
		if (executorList == null) {
			executorList = new ArrayList<Executor>();
			Integer workerCount = workerCountConstraint.get(executor.getTopology());
			if (workerCount == null)
				workerCount = Integer.valueOf(0);
			if (workerCount == executor.getTopology().getWorkerCount())
				throw new RuntimeException(
					"Worker count constraint violated for topology " + executor.getTopology() + " (" + executor.getTopology().getWorkerCount() + "): " +
					"executor " + executor + " cannot be allocated to worker " + worker);
			workerCountConstraint.put(executor.getTopology(), workerCount + 1);
			allocation.put(worker, executorList);
			topoToWorkerConstraint.put(worker, executor.getTopology());
		} else {
			if (!topoToWorkerConstraint.get(worker).equals(executor.getTopology()))
				throw new RuntimeException(
					"Worker " + worker + " is already assigned to topology " + topoToWorkerConstraint.get(worker) + ": " +
					"it cannot be assigned to executor " + executor + " of topology " + executor.getTopology());
		}
		executorList.add(executor);
		allocatedExecutorMap.put(executor, worker);
	}
	
	public void deallocate(Executor executor) {
		toString = null;
		Worker currentAllocation = allocatedExecutorMap.get(executor);
		if (currentAllocation == null)
			throw new RuntimeException("Executor " + executor + " is not allocated yet");
		allocatedExecutorMap.remove(executor);
		List<Executor> executorList = allocation.get(currentAllocation);
		executorList.remove(executor);
		if (executorList.isEmpty()) {
			allocation.remove(currentAllocation);
			topoToWorkerConstraint.remove(currentAllocation);
			int workerCount = workerCountConstraint.get(executor.getTopology()) - 1;
			if (workerCount == 0)
				workerCountConstraint.remove(executor.getTopology());
			else
				workerCountConstraint.put(executor.getTopology(), workerCount);
		}
	}
	
	public boolean workersForTopologiesContraintSatisfied() {
		boolean satisfied = true;
		for (Topology topology : workerCountConstraint.keySet()) {
			Integer actualWorkerCount = workerCountConstraint.get(topology);
			if (actualWorkerCount == null)
				actualWorkerCount = Integer.valueOf(0);
			int requestedWorkerCount = Math.min(topology.getWorkerCount(), topology.getExecutors().size());
			if (actualWorkerCount != requestedWorkerCount) {
				logger.debug(
					"Worker count constraint violated for topology " + topology + ": " +
					requestedWorkerCount + "requested worker count but " + actualWorkerCount + " actual worker count");
				satisfied = false;
			}
		}
		return satisfied;
	}
	
	public void computeWeight() {
		logger.trace("*** Start computing weight for this allocation (" + allocatedExecutorMap.keySet().size() + " allocated executors)");
		logger.trace(this);
		// compute node loads
		SummaryStatistics statistics = new SummaryStatistics();
		for (Node node : stormCluster.getNodes()) {
			long nodeLoad = 0;
			for (Worker worker : node.getWorkers()) {
				List<Executor> executorList = allocation.get(worker);
				if (executorList != null)
					for (Executor executor : executorList) {
						nodeLoad += executor.getLoad();
						logger.trace("Load of " + executor + ": " + executor.getLoad() + " CPU cycles");
					}
			}
			double nodeLoadPerc = (double)nodeLoad / node.getSpeed();
			logger.trace("Load of node " + node.getHostname() + ": " + nodeLoadPerc + "%");
			statistics.addValue(nodeLoadPerc); // load in %
		}
		
		// compute traffic stats
		totalTraffic = 0;
		interNodeTraffic = 0;
		interSlotTraffic = 0;
		for (Executor srcExecutor : allocatedExecutorMap.keySet()) {
			for (Executor dstExecutor : srcExecutor.getOutputTrafficMap().keySet()) {
				int traffic = srcExecutor.getOutputTrafficMap().get(dstExecutor);
				if (traffic > 0) {
					logger.trace("Traffic from " + srcExecutor + " to " + dstExecutor + ": " + traffic + " t/s (partial total traffic: " + totalTraffic + " t/s)");
					Worker srcWorker = allocatedExecutorMap.get(srcExecutor);
					logger.trace("Source slot: " + srcWorker);
					Worker dstWorker = allocatedExecutorMap.get(dstExecutor);
					logger.trace("Destination slot: " + dstWorker);
					if (srcWorker != null && dstWorker != null) { // accommodate for partial allocations
						Node srcNode = srcWorker.getNode();
						logger.trace("Source node: " + srcNode);
						Node dstNode = dstWorker.getNode();
						logger.trace("Destination node: " + dstNode);
						totalTraffic += traffic;
						if (!srcNode.equals(dstNode)) { // node is different
							interNodeTraffic += traffic;
							logger.trace("Traffic added to inter-node traffic");
						} else if (!srcWorker.equals(dstWorker)) { // node is equal but slot is different
							interSlotTraffic += traffic;
							logger.trace("Traffic added to inter-slot traffic");
						}
						logger.trace("Partial inter-node-traffic: " + interNodeTraffic + " t/s; partial inter-slot-traffic: " + interSlotTraffic + " t/s");
					}
				}
			}
		}
		
		// compute partials
		double alfa = SchedulerConf.getInstance().getAlfa(); logger.trace("alfa: " + alfa);
		double beta = SchedulerConf.getInstance().getBeta(); logger.trace("beta: " + beta);
		
		meanLoad = statistics.getMean(); logger.trace("loadMean: " + meanLoad);
		loadStdev = statistics.getStandardDeviation(); logger.trace("loadStdev: " + loadStdev);
		double loadTerm = (meanLoad > 0)?Math.pow(alfa * loadStdev / meanLoad, 2):0; logger.trace("loadTerm: " + loadTerm);
		
		logger.trace("inter-node traffic: " + interNodeTraffic + " t/s");
		logger.trace("inter-sot traffic: " + interSlotTraffic + " t/s");
		logger.trace("total traffic: " + totalTraffic + " t/s");
		
		double interNodeTerm = (totalTraffic > 0)?(beta * interNodeTraffic / totalTraffic):0; logger.trace("interNodeTerm: " + interNodeTerm);
		double interSlotTerm = (totalTraffic > 0)?((1 - beta) * interSlotTraffic / totalTraffic):0; logger.trace("interSlotTerm: " + interSlotTerm);
		double trafficTerm = Math.pow( (1 - alfa) * (interNodeTerm + interSlotTerm), 2); logger.trace("trafficTerm: " + trafficTerm);

		weight = Math.sqrt(loadTerm + trafficTerm); logger.trace("weight: " + weight + " (" + getMeaningfulnessMsg() + ")");
	}

	public double getWeight() {
		return weight;
	}
	
	public boolean isWeightMeaningful() {
		return meanLoad > 0 || totalTraffic > 0;
	}
	
	public String getMeaningfulnessMsg() {
		if (isWeightMeaningful())
			return "meaningful: yes";
		else if (meanLoad == 0 && totalTraffic == 0)
			return "meaningful: no because both mean load and total traffic are zero";
		else if (meanLoad == 0)
			return "meaningful: no because mean load is zero";
		else 
			return "meaningful: no because total traffic is zero";
	}
	
	@Override
	public String toString() {
		if (toString == null) {
			StringBuffer sb = new StringBuffer();
			for (Worker worker : allocation.keySet()) {
				List<Executor> executorList = allocation.get(worker);
				if (executorList != null) {
					sb.append(worker.toString());
					sb.append("{");
					for (Executor executor : executorList)
						sb.append(executor.toString() + ", ");
					sb.append("};");
				}
			}
			toString = sb.toString();
		}
		return toString;
	}

	public double getMeanLoad() {
		return meanLoad;
	}

	public double getLoadStdev() {
		return loadStdev;
	}

	public long getTotalTraffic() {
		return totalTraffic;
	}

	public long getInterNodeTraffic() {
		return interNodeTraffic;
	}

	public long getInterSlotTraffic() {
		return interSlotTraffic;
	}
}
