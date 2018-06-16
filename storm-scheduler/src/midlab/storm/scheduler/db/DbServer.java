package midlab.storm.scheduler.db;

import java.net.InetAddress;

import org.apache.derby.drda.NetworkServerControl;
import org.apache.log4j.Logger;

public class DbServer {

	private static DbServer instance = null;

	private Logger logger;
	
	public static DbServer getInstance() {
		if (instance == null)
			instance = new DbServer();
		return instance;
	}
	
	private DbServer() {
		try {
			logger = Logger.getLogger(DbServer.class);
			NetworkServerControl serverControl = new NetworkServerControl(InetAddress.getByName("0.0.0.0"), DbConf.getInstance().getDbPort());
			serverControl.start(null);
		} catch (Exception e) {
			logger.fatal("Cannot start the DB server", e);
			System.exit(-1);
		}
	}

}
