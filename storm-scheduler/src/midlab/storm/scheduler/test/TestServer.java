package midlab.storm.scheduler.test;

import midlab.storm.scheduler.db.DbServer;
import midlab.storm.scheduler.db.SchedulerDataManager;

public class TestServer {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		System.out.println(System.getenv("DERBY_HOME"));
		DbServer.getInstance();
		SchedulerDataManager.getInstance();
		while (true)
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
	}

}
