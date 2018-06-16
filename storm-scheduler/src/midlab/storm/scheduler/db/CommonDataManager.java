package midlab.storm.scheduler.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import org.apache.log4j.Logger;

public class CommonDataManager {

	private static CommonDataManager instance = null;
	
	private Logger logger;
	private Connection conn;
	
	public static CommonDataManager getInstance() {
		if (instance == null)
			instance = new CommonDataManager();
		return instance;
	}
	
	private CommonDataManager() {
		logger = Logger.getLogger(CommonDataManager.class);
		try {
			Class.forName(DbConf.getInstance().getDbDriver());
			logger.info("Driver loaded");
			String connURL = DbConf.getInstance().getDbClientConnectionURL();
			logger.info("Connection URL: " + connURL);
			conn = DriverManager.getConnection(connURL);
			logger.info("Connected to DB " + DbConf.getInstance().getDbName());
		} catch(Exception e) {
			logger.error("Error connecting to DB", e);
		}
	}
	
	public void updateWorkerLoad(String hostname, int port, int cpuUsage) throws Exception {
		Statement statement = null;
		String sql = "update worker set cpu_usage = " + cpuUsage + ", ts = current_timestamp where hostname = '" + hostname + "' and port = " + port;
		try {
			statement = conn.createStatement();
			if (statement.executeUpdate(sql) == 0) {
				sql = "insert into worker(hostname, port, cpu_usage) values('" + hostname + "', " + port + ", " + cpuUsage + ")";
				statement.execute(sql);
			}
		} catch(Exception e) {
			logger.error("SQL error while updating worker load", e);
			logger.error("SQL script: " + sql);
		} finally {
			if (statement != null)
				statement.close();
		}
	}
	
	public void updateLoad(String topologyId, int taskId, long load, String workerHostname, int workerPort) throws Exception {
		Statement statement = null;
		String sql = "update load set load = " + load + ", worker_hostname = '" + workerHostname + "', worker_port = " + workerPort +  ", ts = current_timestamp where topology_id = '" + topologyId + "' and task_id = " + taskId;
		try {
			statement = conn.createStatement();
			if (statement.executeUpdate(sql) == 0) {
				sql = "insert into load(topology_id, task_id, load, worker_hostname, worker_port) values('" + topologyId + "', " + taskId + ", " + load + ", '" + workerHostname + "'," + workerPort + ")";
				statement.execute(sql);
			}
		} catch(Exception e) {
			logger.error("SQL error while updating the load", e);
			logger.error("SQL script: " + sql);
		} finally {
			if (statement != null)
				statement.close();
		}
	}
	
	public void updateTraffic(String topologyId, int srcTaskId, int dstTaskId, int traffic) throws Exception {
		Statement statement = null;
		String sql = "update traffic set traffic = " + traffic + ", ts = current_timestamp where " +
				"topology_id = '" + topologyId + "' and source_task_id = " + srcTaskId + " and destination_task_id = " + dstTaskId;
		try {
			statement = conn.createStatement();
			if (statement.executeUpdate(sql) == 0) {
				sql = "insert into traffic(topology_id, source_task_id, destination_task_id, traffic) " +
						"values('" + topologyId + "', " + srcTaskId + ", " + dstTaskId + ", " + traffic + ")";
				statement.execute(sql);
			}
		} catch(Exception e) {
			logger.error("SQL error while updating the traffic", e);
			logger.error("SQL script: " + sql);
		} finally {
			if (statement != null)
				statement.close();
		}
	}
	
	public void updateTupleSize(String topologyId, int srcTaskId, int dstTaskId, long tupleSizeSum, long tupleSizeCount) throws Exception {
		Statement statement = null;
		String sql = "update tuplesize set tuplesizesum = " + tupleSizeSum + ", tuplesizecount = " + tupleSizeCount + ", ts = current_timestamp where " +
				"topology_id = '" + topologyId + "' and source_task_id = " + srcTaskId + " and destination_task_id = " + dstTaskId;
		try {
			statement = conn.createStatement();
			if (statement.executeUpdate(sql) == 0) {
				sql = "insert into tuplesize(topology_id, source_task_id, destination_task_id, tuplesizesum, tuplesizecount) " +
						"values('" + topologyId + "', " + srcTaskId + ", " + dstTaskId + ", " + tupleSizeSum + ", " + tupleSizeCount + ")";
				statement.execute(sql);
			}
		} catch(Exception e) {
			logger.error("SQL error while updating the tuple size", e);
			logger.error("SQL script: " + sql);
		} finally {
			if (statement != null)
				statement.close();
		}
	}
	
}
