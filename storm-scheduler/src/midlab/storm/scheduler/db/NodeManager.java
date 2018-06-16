package midlab.storm.scheduler.db;

import java.net.InetAddress;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;


import org.apache.log4j.Logger;

import cpuinfo.CPUInfo;

/**
 * This singleton is in charge of updating node info on DB
 * 
 * @author Leonardo
 *
 */
public class NodeManager {
	
	private static NodeManager instance = null;
	
	public static NodeManager update() {
		if (instance == null)
			instance = new NodeManager();
		return instance;
	}
	
	private NodeManager() {
		Logger logger = Logger.getLogger(NodeManager.class);
		Connection conn = null;
		Statement stat = null;
		String sql = null;
		try {
			// connect to DB
			Class.forName(DbConf.getInstance().getDbDriver());
			logger.info("Driver loaded");
			String connURL = DbConf.getInstance().getDbClientConnectionURL();
			logger.info("Connection URL: " + connURL);
			conn = DriverManager.getConnection(connURL);
			logger.info("Connected to DB " + DbConf.getInstance().getDbName());
			
			stat = conn.createStatement();
			
			// store node info
			String hostname = InetAddress.getLocalHost().getHostName();
			int coreCount = CPUInfo.getInstance().getNumberOfCores();
			long coreSpeed = CPUInfo.getInstance().getCoreInfo(0).getSpeed();
			sql = "update node set " +
					"core_count = " + coreCount + ", " +
					"core_speed = " + coreSpeed + " " +
					"where hostname = '" + hostname + "'";
			int updated = stat.executeUpdate(sql);
			if (updated == 0) {
				sql = "insert into node(hostname, core_count, core_speed) " +
						"values('" + hostname + "', " + coreCount + ", " + coreSpeed + ")";
				stat.execute(sql);
			}
			
		} catch(Exception e) {
			logger.error("Error in NodeManager to DB", e);
			logger.error("SQL script: " + sql);
		} finally {
			if (stat != null)
				try { stat.close(); } catch (SQLException e) { logger.error("Error closing the statement", e);	}
			if (conn != null)
				try { conn.close(); } catch (SQLException e) { logger.error("Error closing the connection", e);	}
		}
	}
}
