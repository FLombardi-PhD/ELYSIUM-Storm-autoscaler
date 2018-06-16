package midlab.storm.scheduler.data;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.TopologyDetails;

public class Topology {
	
	private final String id;
	private final String name;
	private final int workerCount;
	private final Map<Integer, Executor> taskToExecutorMap;
	private String toString;

	public Topology(TopologyDetails td) {
		id = td.getId();
		name = td.getName();
		workerCount = td.getNumWorkers();
		taskToExecutorMap = new HashMap<Integer, Executor>();
		for (ExecutorDetails ed : td.getExecutors()) {
			Executor executor = new Executor(this, td, ed);
			for (int i = executor.getStartTaskId(); i <= executor.getEndTaskId(); i++)
				taskToExecutorMap.put(i, executor);
		}
	}
	
	public String getId() {
		return id;
	}
	
	public String getName() {
		return name;
	}
	
	public int getWorkerCount() {
		return workerCount;
	}
	
	public Collection<Executor> getExecutors() {
		Collection<Executor> executorsListWithDuplicates = taskToExecutorMap.values();
		List<Executor> executorsListWithoutDuplicates = new LinkedList<Executor>(); 
		for (Executor e : executorsListWithDuplicates) {
			if (!executorsListWithoutDuplicates.contains(e))
				executorsListWithoutDuplicates.add(e);
		}
		return executorsListWithoutDuplicates;
	}
	
	/*
	 * OLD METHOD: it list all executors with duplicated values in case
	 * that # task != # executors
	public Collection<Executor> getExecutors() {
		return taskToExecutorMap.values();
	}
	*/
	
	public Executor getExecutorByTaskId(int taskId) {
		return taskToExecutorMap.get(taskId);
	}
	
	@Override
	public String toString() {
		if (toString == null) {
			StringBuffer sb = new StringBuffer();
			sb.append(id);
			sb.append("[worker count " + workerCount + "] {");
			for (Executor executor : taskToExecutorMap.values())
				sb.append(executor.toString() + ", ");
			sb.append("}");
			toString = sb.toString();
		}
		return toString;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!(obj instanceof Topology))
			return false;
		Topology other = (Topology) obj;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		return true;
	}
}
