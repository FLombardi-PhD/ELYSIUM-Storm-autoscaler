package midlab.storm.scheduler.data;

import java.util.HashMap;
import java.util.Map;

import midlab.storm.scheduler.SchedulerConf;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.TopologyDetails;

public class Executor implements Comparable<Executor> {
	
	private final ExecutorKey key;
	private final ExecutorDetails stormExecutor;
	private final Topology topology;
	private long load;
	private Map<Executor, Integer> outputTrafficMap;
	private int traffic;
	private double weight;
	
	public Executor(Topology topology, TopologyDetails stormTopology, ExecutorDetails stormExecutor) {
		this.key = new ExecutorKey(
			stormTopology.getId(), // topology ID
			stormTopology.getExecutorToComponent().get(stormExecutor), // component ID 
			stormExecutor.getStartTask(), // executor start task ID 
			stormExecutor.getEndTask()); // executor end task ID
		this.stormExecutor = stormExecutor;
		this.topology = topology;
		outputTrafficMap = new HashMap<Executor, Integer>();
	}
	
	public Topology getTopology() {
		return topology;
	}
	
	public ExecutorDetails getStormExecutor() {
		return stormExecutor;
	}
	
	public int getStartTaskId() {
		return key.getStartTaskId();
	}
	
	public int getEndTaskId() {
		return key.getEndTaskId();
	}
	
	public void setLoad(long load) {
		this.load = load;
	}
	
	/**
	 * @return executor load (in CPU cycles) in the last time window
	 */
	public long getLoad() {
		return load;
	}
	
	/**
	 * set traffic from this exec to param exec
	 * 
	 * @param dest
	 * @param traffic
	 */
	public void addOutputTraffic(Executor exec, int traffic) {
		outputTrafficMap.put(exec, traffic);
		this.traffic += traffic;
	}
	
	/**
	 * set traffic from param exec to this exec
	 * 
	 * @param dest
	 * @param traffic
	 */
	public void addInputTraffic(Executor exec, int traffic) {
		this.traffic += traffic;
	}
	
	public int getTraffic() {
		return traffic;
	}
	
	public void setWeightParams(long totalLoad, long totalTraffic) {
		double alfa = SchedulerConf.getInstance().getAlfa();
		weight = alfa * ((double)load / totalLoad) + (1 -alfa) * ((double)traffic / totalTraffic);
	}
	
	public double getWeight() {
		return weight;
	}

	@Override
	public int compareTo(Executor o) {
		return Double.valueOf(weight).compareTo(Double.valueOf(o.getWeight()));
	}

	public ExecutorKey getKey() {
		return key;
	}

	@Override
	public int hashCode() {
		return key.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!(obj instanceof Executor))
			return false;
		Executor other = (Executor) obj;
		if (key == null) {
			if (other.key != null)
				return false;
		} else if (!key.equals(other.key))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return key.toString();
	}

	public Map<Executor, Integer> getOutputTrafficMap() {
		return outputTrafficMap;
	}
}
