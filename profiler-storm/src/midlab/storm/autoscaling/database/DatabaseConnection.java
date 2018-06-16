package midlab.storm.autoscaling.database;

import java.io.File;
import java.io.FileReader;
import java.io.InputStream;
import java.sql.DriverManager;
import java.sql.Connection;
import java.util.Properties;

import midlab.storm.autoscaling.profiler.SelectivityProfiler;

/**
 * Class to connect to a derby database
 * @author Federico Lombardi - Sapienza University of Rome
 *
 */
public class DatabaseConnection {

	public static final String propFileName = "topology.properties";
	
	public static Connection connect() throws Exception{
		
		Properties prop = new Properties();
		InputStream inputStream = SelectivityProfiler.class.getClassLoader().getResourceAsStream(propFileName);
		prop.load(inputStream);
		
		String ipAddress = prop.getProperty("ip").replaceAll(" ", "");
		
		Class.forName("org.apache.derby.jdbc.ClientDriver");
		
		Connection connection = null;
	 
		connection = (Connection) DriverManager.getConnection("jdbc:derby://"+ipAddress+":1527/storm-scheduler");
	 
		if (connection != null) {
			return connection;
		} else {
			System.out.println("Failed to make connection!");
			return null;
		}
	}
	
	public static Connection connect(String dataDirectory) throws Exception{
		
		Properties prop = new Properties();
		//InputStream inputStream = SelectivityProfiler.class.getClassLoader().getResourceAsStream(propFileName);
		File dataDirFile = new File(dataDirectory);
		File propFile = new File(dataDirFile, propFileName);
		prop.load(new FileReader(propFile));
		
		String ipAddress = prop.getProperty("ip").replaceAll(" ", "");
		
		Class.forName("org.apache.derby.jdbc.ClientDriver");
		
		Connection connection = null;
	 
		connection = (Connection) DriverManager.getConnection("jdbc:derby://"+ipAddress+":1527/storm-scheduler");
	 
		if (connection != null) {
			return connection;
		} else {
			System.out.println("Failed to make connection!");
			return null;
		}
	}
	
}
