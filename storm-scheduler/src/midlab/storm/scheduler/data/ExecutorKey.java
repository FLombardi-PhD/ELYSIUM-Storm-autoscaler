package midlab.storm.scheduler.data;

public class ExecutorKey {

	private String topologyId;
	private String componentId;
	private int startTaskId;
	private int endTaskId;
	private String toString;

	public ExecutorKey(String topologyId, String componentId, int beginTaskId, int endTaskId) {
		this.topologyId = topologyId;
		this.componentId = componentId;
		this.startTaskId = beginTaskId;
		this.endTaskId = endTaskId;
	}

	public String getTopologyId() {
		return topologyId;
	}
	
	public String getComponentId() {
		return componentId;
	}

	public int getStartTaskId() {
		return startTaskId;
	}

	public int getEndTaskId() {
		return endTaskId;
	}
	
	@Override
	public String toString() {
		if (toString == null)
			toString = componentId + "[" + startTaskId + "-" + endTaskId + "]";
		return toString;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + startTaskId;
		result = prime * result
				+ ((componentId == null) ? 0 : componentId.hashCode());
		result = prime * result + endTaskId;
		result = prime * result
				+ ((topologyId == null) ? 0 : topologyId.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!(obj instanceof ExecutorKey))
			return false;
		ExecutorKey other = (ExecutorKey) obj;
		if (startTaskId != other.startTaskId)
			return false;
		if (componentId == null) {
			if (other.componentId != null)
				return false;
		} else if (!componentId.equals(other.componentId))
			return false;
		if (endTaskId != other.endTaskId)
			return false;
		if (topologyId == null) {
			if (other.topologyId != null)
				return false;
		} else if (!topologyId.equals(other.topologyId))
			return false;
		return true;
	}
	
	
}
