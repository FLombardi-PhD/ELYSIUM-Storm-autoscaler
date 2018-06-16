package midlab.storm.scheduler.test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import midlab.storm.scheduler.data.StormCluster;
import midlab.storm.scheduler.data.Topology;
import midlab.storm.scheduler.db.DbConf;
import midlab.storm.scheduler.db.SchedulerDataManager;

import backtype.storm.Config;
import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.SchedulerAssignmentImpl;
import backtype.storm.scheduler.SupervisorDetails;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;

public class TestSchedulerDataManager {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		DbConf.getInstance().setNimbusHost("vm-aniello-storm-00");
		Map<String, Topology> topologyMap = SchedulerDataManager.getInstance().loadData(createTopologies(args[0]));
		System.out.println("Loaded data: " + topologyMap);
		StormCluster cluster = SchedulerDataManager.getInstance().loadCluster(createCluster());
		System.out.println("Loaded cluster: " + cluster);
	}
	
	private static Cluster createCluster() {
		Map<String, SupervisorDetails> supervisors = new HashMap<String, SupervisorDetails>();
		for (int i = 1; i <= 4; i++) {
			Collection<Number> ports = new ArrayList<Number>();
			for (int p = 6700; p <= 6703; p++)
				ports.add(p);
			SupervisorDetails supervisor = new SupervisorDetails("vm-aniello-storm-0"+i, "vm-aniello-storm-0"+i, null, ports);
			supervisors.put(supervisor.getId(), supervisor);
		}
		Map<String, SchedulerAssignmentImpl> assignments = new HashMap<String, SchedulerAssignmentImpl>();
		Cluster cluster = new Cluster(null, supervisors, assignments);
		return cluster;
	}

	private static Topologies createTopologies(String topologyID) {
		Map<String, TopologyDetails> topologyMap = new HashMap<String, TopologyDetails>();
		
		TopologyDetails topology = new TopologyDetails(topologyID, getConf(), null, 0, getExecutorToComponents());
		topologyMap.put(topology.getId(), topology);
		
		return new Topologies(topologyMap);
	}
	
	private static Map<ExecutorDetails, String> getExecutorToComponents() {
		
		Map<ExecutorDetails, String> executorToComponents = new HashMap<ExecutorDetails, String>();
		
		// acker
		ExecutorDetails acker = new ExecutorDetails(1, 1);
		executorToComponents.put(acker, "__acker");
		
		// metrics
		ExecutorDetails metrics = new ExecutorDetails(2, 2);
		executorToComponents.put(metrics, "__metricsmidlab.storm.scheduler.SchedulerMetricsConsumer");
		
		// random spout
		for (int i = 8; i <= 9; i++) {
			ExecutorDetails random = new ExecutorDetails(i, i);
			executorToComponents.put(random, "random");
		}
		
		// avg bolt
		for (int i = 3; i <= 7; i++) {
			ExecutorDetails avg = new ExecutorDetails(i, i);
			executorToComponents.put(avg, "avg");
		}
		
		// sum bolt
		for (int i = 10; i <= 14; i++) {
			ExecutorDetails sum = new ExecutorDetails(i, i);
			executorToComponents.put(sum, "sum");
		}
		
		return executorToComponents;
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private static Map getConf() {
		Map conf = new HashMap();
		conf.put(Config.TOPOLOGY_NAME, "dummy");
		return conf;
	}
}
