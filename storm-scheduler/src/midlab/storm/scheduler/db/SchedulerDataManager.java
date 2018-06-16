package midlab.storm.scheduler.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import midlab.storm.scheduler.data.Executor;
import midlab.storm.scheduler.data.Node;
import midlab.storm.scheduler.data.StormCluster;
import midlab.storm.scheduler.data.Topology;

import org.apache.log4j.Logger;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.SupervisorDetails;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;

public class SchedulerDataManager {

	private static SchedulerDataManager instance = null;
	
	private static final String CREATE_LOAD_TABLE =
			"create table load (" +
			"	topology_id varchar(256) not null," +
			"	task_id int not null," +
			"	ts timestamp not null default current_timestamp, " +
			"	worker_hostname varchar(256) not null," +
			"	worker_port int not null," +
			"	load bigint not null" +
			")";
	
	private static final String CREATE_TRAFFIC_TABLE = 
			"create table traffic (" +
			"	topology_id varchar(256) not null," +
			"	source_task_id int not null," +
			"	destination_task_id int not null," +
			"	ts timestamp not null default current_timestamp," +
			"	traffic int not null" +
			")";
	
	private static final String CREATE_WORKER_TABLE = 
			"create table worker (" +
			"	hostname varchar(256) not null," +
			"	port int not null," +
			"	ts timestamp not null default current_timestamp," +
			"	cpu_usage int not null" +
			")";
	
	private static final String CREATE_NODE_TABLE = 
			"create table node (" +
			"	hostname varchar(256) not null," +
			"	core_count int not null," +
			"	core_speed bigint not null" +
			")";
	
	private static final String CREATE_TUPLE_SIZE_TABLE =
			"create table tuplesize (" +
			"	topology_id varchar(256) not null," +
			"	source_task_id int not null," +
			"	destination_task_id int not null," +
			"	ts timestamp not null default current_timestamp," +
			"	tuplesizesum bigint not null," +
			"	tuplesizecount bigint not null" +
			")";
	
	private Logger logger;
	private Connection conn;
	
	public static SchedulerDataManager getInstance() {
		if (instance == null)
			instance = new SchedulerDataManager();
		return instance;
	}
	
	public SchedulerDataManager() {
		logger = Logger.getLogger(SchedulerDataManager.class);
		try {
			Class.forName(DbConf.getInstance().getDbDriver());
			logger.info("Driver loaded");
		} catch(ClassNotFoundException e) {
			logger.fatal("Cannot load DB driver", e);
			System.exit(-1);
		}
		
		Statement statement = null;
		String sql = null;
		
		try {
			conn = DriverManager.getConnection(DbConf.getInstance().getDbServerConnectionURL());
			logger.info("Connected to DB " + DbConf.getInstance().getDbName());
			
			statement = conn.createStatement();
			
			// create load table
			if (!loadTableExists()) {
				sql = CREATE_LOAD_TABLE;
				statement.execute(sql);
				logger.info("Load table created");
			}
			
			// create traffic table
			if (!trafficTableExsits()) {
				sql = CREATE_TRAFFIC_TABLE;
				statement.execute(sql);
				logger.info("Traffic table created");
			}
			
			// create worker table
			if (!workerTableExists()) {
				sql = CREATE_WORKER_TABLE;
				statement.execute(sql);
				logger.info("Worker table created");
			}
			
			// create node table
			if (!nodeTableExists()) {
				sql = CREATE_NODE_TABLE;
				statement.execute(sql);
				logger.info("Node table created");
			}
			
			// create tuplesize table
			if (!tupleSizeTableExists()) {
				sql = CREATE_TUPLE_SIZE_TABLE;
				statement.execute(sql);
				logger.info("Tuple Size table created");
			}
		} catch(Exception e) {
			logger.fatal("Error booting DB", e);
			if (sql != null)
				logger.error("SQL script: " + sql);
			System.exit(-1);
		} finally {
			if (statement != null)
				try { statement.close(); }
				catch (SQLException e) { logger.error("Error closing the statement", e); }
		}
	}
	
	public void cleanDB(List<String> runningTopologyList) throws Exception {
		Statement statement = null;
		String sql = null;
		
		try {
			
			statement = conn.createStatement();
			
			String topoList = "(";
			for (int i = 0; i < runningTopologyList.size(); i++) {
				topoList += "'" + runningTopologyList.get(i) + "'";
				if (i < runningTopologyList.size() - 1)
					topoList += ", ";
			}
			topoList += ")";
			
			// load table
			sql = "delete from load";
			if (!runningTopologyList.isEmpty())
				sql += " where topology_id not in " + topoList;
			int removedLoadEntryCount = statement.executeUpdate(sql);
			logger.debug("Removed " + removedLoadEntryCount + " entries from load table");
			
			// traffic table
			sql = "delete from traffic";
			if (!runningTopologyList.isEmpty())
				sql += " where topology_id not in " + topoList;
			int removedTrafficEntryCount = statement.executeUpdate(sql);
			logger.debug("Removed " + removedTrafficEntryCount + " entries from traffic table");
			
		} catch(Exception e) {
			logger.error("Error cleaning DB", e);
			if (sql != null)
				logger.error("SQL script: " + sql);
		} finally {
			if (statement != null)
				statement.close();
		}
	}
	
	/**
	 * @param stormTopologies
	 * @param topologyMap
	 * @param topologyToExecutorToLoadUpdateTsMap
	 */
	private void initLoadDataStructures(
			Topologies stormTopologies, 
			Map<String, Topology> topologyMap, // SIDE-EFFECT - topoID -> topology
			Map<String, Map<Integer, Timestamp>> topologyToExecutorToLoadUpdateTsMap) // SIDE-EFFECT - topoID -> executor (=startTaskID) -> ts of last load update
	{
		for (TopologyDetails stormTopology : stormTopologies.getTopologies()) {
			// new entry for topologyMap
			Topology topology = new Topology(stormTopology);
			topologyMap.put(topology.getId(), topology);
			
			// new entry for topologyAndExecutorToLoadUpdateTsMap
			Map<Integer, Timestamp> topologyLoadUpdateTS = new HashMap<Integer, Timestamp>();
			topologyToExecutorToLoadUpdateTsMap.put(stormTopology.getId(), topologyLoadUpdateTS);
			
			// populate topologyToExecutorToLoadUpdateTsMap
			for (Executor executor : topology.getExecutors())
				topologyLoadUpdateTS.put(executor.getStartTaskId(), new Timestamp(0));
		}
	}
	
	/**
	 * @param topologies
	 * @return
	 * @throws Exception
	 */
	public Map<String, Topology> loadData(Topologies topologies) throws Exception {
		Statement statement = null;
		ResultSet resultSet = null;
		String sql = null;
		Map<String, Topology> topologyMap = null;
		try {
			
			// init required data structures
			topologyMap = new HashMap<String, Topology>();
			Map<String, Map<Integer, Timestamp>> topologyToExecutorToLoadUpdateTsMap = new HashMap<String, Map<Integer,Timestamp>>();
			initLoadDataStructures(topologies, topologyMap, topologyToExecutorToLoadUpdateTsMap); 
						
			statement = conn.createStatement();
			
			long totalLoad = 0;
			int totalTraffic = 0;
			
			// load load data and set load into executors
			sql = "select topology_id, task_id, ts, load from load";
			resultSet = statement.executeQuery(sql);
			String topologyId; int taskId; Timestamp ts; long load;
			while (resultSet.next()) {
				
				// read record
				topologyId = resultSet.getString(1);
				taskId = resultSet.getInt(2);
				ts = resultSet.getTimestamp(3);
				load = resultSet.getLong(4);
				
				if (taskId > 0) {
					// identify executor
					Executor executor = topologyMap.get(topologyId).getExecutorByTaskId(taskId);
					
					// check timestamp to update load
					Timestamp currentTS = topologyToExecutorToLoadUpdateTsMap.get(topologyId).get(executor.getStartTaskId());
					if (ts.compareTo(currentTS) > 0) {
						topologyToExecutorToLoadUpdateTsMap.get(topologyId).put(executor.getStartTaskId(), ts);
						totalLoad -= executor.getLoad();
						executor.setLoad(load);
						totalLoad += load;
					}
				}
			}
			resultSet.close();
			
			// load traffic data and set traffic info to executors
			sql = "select topology_id, source_task_id, destination_task_id, ts, traffic from traffic";
			resultSet = statement.executeQuery(sql);
			int srcTaskId, dstTaskId, traffic;
			while (resultSet.next()) {
				
				// read record
				topologyId = resultSet.getString(1);
				srcTaskId = resultSet.getInt(2);
				dstTaskId = resultSet.getInt(3);
				ts = resultSet.getTimestamp(4);
				traffic = resultSet.getInt(5);
				
				// identify executors
				Executor srcExecutor = topologyMap.get(topologyId).getExecutorByTaskId(srcTaskId);
				Executor dstExecutor = topologyMap.get(topologyId).getExecutorByTaskId(dstTaskId);
				srcExecutor.addOutputTraffic(dstExecutor, traffic);
				dstExecutor.addInputTraffic(srcExecutor, traffic);
				totalTraffic += traffic;
			}
			resultSet.close();
			
			// set total load and traffic to all the executors
			for (Topology topology : topologyMap.values())
				 for (Executor executor : topology.getExecutors())
					 executor.setWeightParams(totalLoad, totalTraffic);
			
		} catch(Exception e) {
			logger.error("Error loading data", e);
			if (sql != null)
				logger.error("SQL script: " + sql);
		} finally {
			if (resultSet != null)
				resultSet.close();
			if (statement != null)
				statement.close();
		}
		return topologyMap;
	}
	
	private boolean loadTableExists() throws Exception {
		return tableExists("load");
	}
	
	private boolean trafficTableExsits() throws Exception {
		return tableExists("traffic");
	}
	
	private boolean workerTableExists() throws Exception {
		return tableExists("worker");
	}
	
	private boolean nodeTableExists() throws Exception {
		return tableExists("node");
	}
	
	private boolean tupleSizeTableExists() throws Exception {
		return tableExists("tuplesize");
	}
	
	private boolean tableExists(String tableName) throws Exception {
		if (conn == null)
			throw new Exception("No connection available to check whether " + tableName + " table exists");
		Statement s = null;
		ResultSet rs = null;
		try {
			s = conn.createStatement();
			rs = s.executeQuery("select * from " + tableName);
		} catch (Exception e) {
			if (s != null)
				s.close();
			return false;
		} finally {
			if (rs != null)
				rs.close();
			if (s != null)
				s.close();
		}
		return true;
	}
	
	/**
	 * Loads all the data related to nodes (worker nodes), workers (slots) but nothing about allocations of executors to slots
	 * This info is not expected to change in time. If a node/slot is added/removed, then the nimbus should be restarted
	 * TODO allow for dynamic changes
	 * 
	 * @param cluster
	 * @return
	 * @throws Exception
	 */
	public StormCluster loadCluster(Cluster cluster) throws Exception {
		Statement statement = null;
		ResultSet resultSet = null;
		String sql = null;
		StormCluster stormCluster = null;
		try {
			statement = conn.createStatement();
			sql = "select hostname, core_count * core_speed as speed from node";
			resultSet = statement.executeQuery(sql);
			List<Node> nodes = new ArrayList<Node>();
			while (resultSet.next()) {
				String hostname = resultSet.getString(1);
				long speed = resultSet.getLong(2);
				List<SupervisorDetails> supervisorList = cluster.getSupervisorsByHost(hostname);
				if (supervisorList.isEmpty()) {
					logger.warn("Nimbus doesn't have supervisor info yet, wait a little bit");
					return null;
				}
				// we assume a single supervisor on each host
				Node node = new Node(supervisorList.get(0), speed);
				nodes.add(node);
			}
			stormCluster = new StormCluster(nodes);
		} catch(Exception e) {
			logger.error("Error loading cluster from DB", e);
			if (sql != null)
				logger.error("SQL script: " + sql);
		} finally {
			if (resultSet != null)
				resultSet.close();
			if (statement != null)
				statement.close();
		}
		return stormCluster;
	}
}
