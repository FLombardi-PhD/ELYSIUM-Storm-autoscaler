package midlab.storm.scheduler.data;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class StormCluster {

	/**
	 * hostname -> node
	 */
	private Map<String, Node> hostnameToNodeMap;
	
	private Map<String, Node> idToNodeMap;
	
	public StormCluster(Collection<Node> nodes) {
		hostnameToNodeMap = new HashMap<String, Node>();
		idToNodeMap = new HashMap<String, Node>();
		for (Node node : nodes) {
			hostnameToNodeMap.put(node.getHostname(), node);
			idToNodeMap.put(node.getId(), node);
		}
	}
	
	public Node getNodeByHostname(String hostname) {
		return hostnameToNodeMap.get(hostname);
	}
	
	public Node getNodeById(String id) {
		return idToNodeMap.get(id);
	}
	
	public Collection<Node> getNodes() {
		return hostnameToNodeMap.values();
	}
	
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		for (Node node : hostnameToNodeMap.values()) {
			sb.append(node.toString());
			sb.append(", ");
		}
		return sb.toString();
	}
}
