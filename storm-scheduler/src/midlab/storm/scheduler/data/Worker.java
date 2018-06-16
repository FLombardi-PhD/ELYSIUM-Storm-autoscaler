package midlab.storm.scheduler.data;

public class Worker {

	private final Node node;
	private final int port;
	private String toString;
	
	public Worker(Node node, int port) {
		this.node = node;
		this.port = port;
	}
	
	public Node getNode() {
		return node;
	}
	
	public int getPort() {
		return port;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((node == null) ? 0 : node.hashCode());
		result = prime * result + port;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!(obj instanceof Worker))
			return false;
		Worker other = (Worker) obj;
		if (node == null) {
			if (other.node != null)
				return false;
		} else if (!node.equals(other.node))
			return false;
		if (port != other.port)
			return false;
		return true;
	}
	
	@Override
	public String toString() {
		if (toString == null)
			toString = node.getHostname() + "[" + port + "]";
		return toString;
	}
}
