package midlab.storm.scheduler;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.log4j.Logger;

import midlab.storm.scheduler.data.Allocation;
import midlab.storm.scheduler.data.Executor;
import midlab.storm.scheduler.data.Node;
import midlab.storm.scheduler.data.StormCluster;
import midlab.storm.scheduler.data.Topology;
import midlab.storm.scheduler.data.Worker;

public class PerfectScheduler implements MidlabScheduler {
	
	private Logger logger = Logger.getLogger(PerfectScheduler.class);

	@Override
	public Allocation schedule(StormCluster stormCluster, Map<String, Topology> topologyMap) {
		
		// prepare the list of all available workers
		List<Worker> allWorkerList = new ArrayList<Worker>();
		for (Node node : stormCluster.getNodes())
			for (Worker worker : node.getWorkers())
				allWorkerList.add(worker);
		Worker workers[] = (Worker[])allWorkerList.toArray();
		
		// prepare the list of all the executors
		List<Executor> allExecutorList = new ArrayList<Executor>();
		for (Topology topology : topologyMap.values())
			for (Executor executor : topology.getExecutors())
				allExecutorList.add(executor);
		Executor executors[] = (Executor[])allExecutorList.toArray();
		
		// initialize the stack
		Allocation bestAllocation = null;
		Allocation tmpAllocation = null;
		int computedAllocationCount = 0;
		int correctAllocationCount = 0;
		int meaningfulAllocationCount = 0;
		Stack<ExecutorAllocation> stack = new Stack<ExecutorAllocation>();
		for (int i = 0; i < executors.length; i++)
			stack.push(new ExecutorAllocation(i, 0));
		
		while (true) {
			
			// pop all 'saturated' elements; if stack becomes empty then the algorithm terminates
			while (!stack.isEmpty() && stack.peek().workerId == workers.length - 1)
				stack.pop();
			if (stack.isEmpty()) break;
			
			// 'increment' top element
			stack.peek().workerId++;
			
			// re-push all the elements previously popped, each initialized to zero
			while (stack.size() < executors.length)
				stack.push(new ExecutorAllocation(stack.size(), 0));
			
			// compute derived allocation
			tmpAllocation = new Allocation(stormCluster);
			try {
				computedAllocationCount++;
				for (ExecutorAllocation ea : stack)
					tmpAllocation.allocate(workers[ea.workerId], executors[ea.executorId]);
				if (!tmpAllocation.workersForTopologiesContraintSatisfied())
					throw new Exception("Used less workers than required");
				correctAllocationCount++;
				tmpAllocation.computeWeight();
				if (tmpAllocation.isWeightMeaningful()) {
					meaningfulAllocationCount++;
					if (bestAllocation == null || bestAllocation.getWeight() > tmpAllocation.getWeight())
						bestAllocation = tmpAllocation;
				}
			} catch (Exception e) {
				logger.debug("Wrong allocation to discard: " + tmpAllocation, e);
			}
		}
		
		logger.debug("Computed allocations: " + computedAllocationCount);
		logger.debug("Correct allocations: " + correctAllocationCount);
		logger.debug("Meaningful allocations: " + meaningfulAllocationCount);
		
		return bestAllocation;
	}

}

class ExecutorAllocation {
	
	public ExecutorAllocation(int e, int w) {
		executorId = e;
		workerId = w;
	}
	
	public int executorId;
	public int workerId;
}
