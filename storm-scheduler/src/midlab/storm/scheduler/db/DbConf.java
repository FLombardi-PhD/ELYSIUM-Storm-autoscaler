package midlab.storm.scheduler.db;


public class DbConf {
	
	private static final String DEFAULT_DB_NAME = "storm-scheduler";
	private static final int DEFAULT_DB_PORT = 1527;
	private static final String DEFAULT_DB_DRIVER = "org.apache.derby.jdbc.EmbeddedDriver";
	
	public static final String DB_NAME_PARAM = "db.name";
	public static final String DB_PORT_PARAM = "db.port";
	public static final String DB_DRIVER_PARAM = "db.driver";
	public static final String DB_SERVER_CONNECTION_URL = "db.server.connection.url";
	public static final String DB_CLIENT_CONNECTION_URL = "db.client.connection.url";
	
	private String dbName;
	private int dbPort;
	private String dbDriver;
	
	private String nimbusHost;
	
	public void setNimbusHost(String nimbusHost) {
		this.nimbusHost = nimbusHost;
	}
	
	private static DbConf instance = null;
	
	public static DbConf getInstance() {
		if (instance == null)
			instance = new DbConf();
		return instance;
	}

	private DbConf() {
		
		// TODO read from file
		
		dbName = DEFAULT_DB_NAME;
		dbPort = DEFAULT_DB_PORT;
		dbDriver = DEFAULT_DB_DRIVER;
	}

	public String getDbName() {
		return dbName;
	}

	public int getDbPort() {
		return dbPort;
	}

	public String getDbDriver() {
		return dbDriver;
	}

	public String getDbServerConnectionURL() {
		if (nimbusHost == null)
			nimbusHost = "localhost";
		return "jdbc:derby://" + nimbusHost + ":" + dbPort + "/" + DbConf.getInstance().getDbName() + ";create=true";
	}

	public String getDbClientConnectionURL() {
		return "jdbc:derby://" + nimbusHost + ":" + dbPort + "/" + DbConf.getInstance().getDbName();
	}
}
