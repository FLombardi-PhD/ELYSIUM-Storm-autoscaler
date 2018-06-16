package midlab.storm.scheduler.data;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import backtype.storm.scheduler.SupervisorDetails;

public class Node {

	private final String hostname;
	private final String id;
	private final long speed;
	private String toString;

	/**
	 * port -> worker
	 */
	private Map<Integer, Worker> workers;
	
	public Node(SupervisorDetails supervisor, long speed) {
		this.hostname = supervisor.getHost();
		this.id = supervisor.getId();
		this.speed = speed;
		workers = new HashMap<Integer, Worker>();
		for (int port : supervisor.getAllPorts())
			workers.put(port, new Worker(this, port));
	}
	
	public String getId() {
		return id;
	}
	
	public String getHostname() {
		return hostname;
	}
	
	public long getSpeed() {
		return speed;
	}
	
	public Collection<Worker> getWorkers() {
		return workers.values();
	}
	
	public Worker getWorker(int port) {
		return workers.get(port);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((hostname == null) ? 0 : hostname.hashCode());
		result = prime * result + (int) (speed ^ (speed >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!(obj instanceof Node))
			return false;
		Node other = (Node) obj;
		if (hostname == null) {
			if (other.hostname != null)
				return false;
		} else if (!hostname.equals(other.hostname))
			return false;
		if (speed != other.speed)
			return false;
		return true;
	}
	
	@Override
	public String toString() {
		if (toString == null)
			toString = hostname + "[speed " + speed + ", workers: " + workers.size() + "]";
		return toString;
	}
}
