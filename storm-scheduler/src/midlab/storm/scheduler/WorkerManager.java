package midlab.storm.scheduler;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import midlab.storm.scheduler.db.CommonDataManager;

import org.apache.log4j.Logger;

import cpuinfo.CPUInfo;

public class WorkerManager extends Thread {

	private static WorkerManager instance = null;
	
	/*public static WorkerManager getInstance() {
		return instance;
	}*/

	public synchronized static void init(int port) {
		try {
			if (instance == null) {
				instance = new WorkerManager(InetAddress.getLocalHost().getHostName(), port);
				instance.start();
			}
		} catch (UnknownHostException e) {
			Logger.getLogger(WorkerManager.class).error("Error getting local hostname", e);
		}
	}
	
	private Logger logger;
	
	private final String hostname;
	private final int port;
	
	private static final String CPU_DATASET_FOLDER = ".";
	private String cpuDatasetFilePrefix;
	
	// private final double timeToCpuCycles;
	
	private long lastTotalCpuTime;
	
	private List<Integer> cpuUsageList;
	
	private WorkerManager(String hostname, int port) {
		this.hostname = hostname;
		this.port = port;
		
		logger = Logger.getLogger(WorkerManager.class);
		
		/*
		 * We have at disposal load measurements expressed in time unit, we 
		 * need to convert them in CPU cycles in order to be independent of
		 * CPU speed.
		 * We want to measure the load in Hz (s^-1), and time measurements are 
		 * in ns (s*10^-9), so we need to divide by 10^-9.
		 * The unit of this constant is s^-2
		 */
		// timeToCpuCycles = (double)CPUInfo.getInstance().getTotalSpeed() / 1000000000L;
		
		cpuUsageList = new ArrayList<Integer>();
		
		cpuDatasetFilePrefix = hostname + ".cpu_dataset.";
	}
	
	@SuppressWarnings("resource")
	@Override
	public void run() {
		
		logger.info("WorkerManager for " + hostname + ":" + port + " started!");
		
		//cpu number (considering the global cpu + each core)
		int cpuNumber = 0;
		
		//read /proc/stat file and identify the number of cpu
		String procStat = "/proc/stat";
		BufferedReader readerProcStat = null;
		try {
			readerProcStat = new BufferedReader(new FileReader(new File(procStat)));
			while(readerProcStat.readLine().startsWith("cpu")){
				++cpuNumber;
			}
			logger.debug("File "+procStat+" read succesfully! Found "+cpuNumber+" cpu");
			
		} catch (FileNotFoundException e1) {
			logger.error("Cannot open "+procStat+" file", e1);
		} catch (IOException e) {
			logger.error("Error while reading file "+procStat+".",e);
		}
		
		// check for the last cpu dataset file to append; if it not exist create a new one
		File folder = new File(CPU_DATASET_FOLDER);
		String files[] = folder.list(new FilenameFilter() {
			@Override
			public boolean accept(File arg0, String arg1) {
				return arg1.startsWith(cpuDatasetFilePrefix);
			}
		});
		
		String cpuDataset = "";
		if (files.length != 0) {
			cpuDataset = getLastCpuDataset(files);
		}
		else
			cpuDataset = cpuDatasetFilePrefix + System.currentTimeMillis() + ".csv";
		
		boolean cpuDatasetCreated = true;
		FileWriter outCpuDataset = null;
		try {
			outCpuDataset = new FileWriter(new File(CPU_DATASET_FOLDER, cpuDataset), true);
			logger.info("Appending cpu values to " + cpuDataset);
		} catch (Exception e) {
			cpuDatasetCreated = true;
			logger.error("Cannot open " + cpuDataset + " file."
					+ "It will not be generated any cpu_dataset file", e);
		}
		
		
		/*
		 * Now start collecting data from file /proc/stat and print on cpu dataset
		 */
		
		
		//previous values of idle/nonIdle/total cpu
		long[] prevNonIdle = new long[cpuNumber];
		long[] prevIdle = new long[cpuNumber];
		long[] prevTotal = new long[cpuNumber];
		boolean firstRead = true;
		
		//'counter' of iterations and 'module' to define how many iterations read cpu time and write it on db
		int counter = 0;
		int module = 10;
		
		while (true) {
			try {
				/**
				 * Every 'module' read cpu total time and write on db
				 */
				if(counter % module == 0){
					//reset counter
					counter = 1;
					long totalCpuTime = getTotalCpuTime();
					logger.debug("Total CPU time: " + totalCpuTime + " ns");
					if (lastTotalCpuTime > 0) {
						// int cpuUsage = (int)(timeToCpuCycles * (totalCpuTime - lastTotalCpuTime) * 100 / CPUInfo.getInstance().getTotalSpeed());
						int cpuUsage = (int)((double)(totalCpuTime - lastTotalCpuTime) * 100 / (SchedulerConf.getInstance().getTimeBucketSize() * 1000000000L * CPUInfo.getInstance().getNumberOfCores()));
						logger.debug("CPU usage in the last time bucket (" + SchedulerConf.getInstance().getTimeBucketSize() + " s): " + cpuUsage + "%");
						cpuUsageList.add(cpuUsage);
						if (cpuUsageList.size() > SchedulerConf.getInstance().getTimeBucketNumber())
							cpuUsageList.remove(0);
					
						int avgCpuUsage = 0;
						for (Integer cu : cpuUsageList)
							avgCpuUsage += cu.intValue();
						avgCpuUsage /= cpuUsageList.size();
						logger.debug("CPU usage in the last time window (" + SchedulerConf.getInstance().getTimeBucketNumber() * SchedulerConf.getInstance().getTimeBucketSize() + " s): " + avgCpuUsage + "%");
					
						CommonDataManager.getInstance().updateWorkerLoad(hostname, port, avgCpuUsage);
						logger.debug("Updated to DB");
					}
					lastTotalCpuTime = totalCpuTime;
				}
				else ++counter;
				
				/**
				 * Build dataset by reading from /proc/stat file
				 * Dataset structure: timestamp,CPU%,CORE_1%,..CORE_N%
				 * 
				 */
				
				//reinitialization of /proc/stat file reader to read the updated values
				try {
					readerProcStat = new BufferedReader(new FileReader(new File(procStat)));
					logger.debug("File "+procStat+" read succesfully.");
				} catch (FileNotFoundException e1) {
					logger.error("Cannot open "+procStat+" file", e1);
				}
				
				//generate a row for dataset with ts,cpu%,core1%,..coren%
				long timestamp = System.currentTimeMillis();
				String datasetRow = ""+timestamp;
				
				//read cpu values for global CPU and for each core
				for(int i=0; i<cpuNumber; ++i){
					String line = readerProcStat.readLine();
					if(line.startsWith("cpu")){
						int offset = 0;
						if(i==0) offset = 1;
						String[] cpuValues = line.split(" ");
						long cpuUserTime = Long.parseLong(cpuValues[1+offset]);
						long cpuNiceTime = Long.parseLong(cpuValues[2+offset]);
						long cpuSystemTime = Long.parseLong(cpuValues[3+offset]);
						long cpuIdleTime = Long.parseLong(cpuValues[4+offset]);
						long cpuIOWait = Long.parseLong(cpuValues[5+offset]);
						long cpuIRQ = Long.parseLong(cpuValues[6+offset]);
						long cpuSoftIRQ = Long.parseLong(cpuValues[7+offset]);
						
						long cpuNonIdle = cpuUserTime + cpuNiceTime + cpuSystemTime + cpuIRQ + cpuSoftIRQ;
						long cpuIdle = cpuIdleTime + cpuIOWait;
						long cpuTotal = cpuNonIdle + cpuIdle;
						
						double cpuUsagePercentage = 0D;
						if(!firstRead) cpuUsagePercentage = (double)((cpuTotal-prevTotal[i]) - (cpuIdle-prevIdle[i])) / (double)(cpuTotal-prevTotal[i]);
						
						String coreIdentifier = "core "+i;
						if(i==0) coreIdentifier = "cpu";
						logger.debug("CPU values for "+coreIdentifier+":\n\t\t\t\t\t user: "+cpuUserTime+"\n\t\t\t\t\t nice: "+cpuNiceTime
								+ "\n\t\t\t\t\t system: "+cpuSystemTime+"\n\t\t\t\t\t idle: "+cpuIdleTime+"\n\t\t\t\t\t IOwait: "+cpuIOWait
								+ "\n\t\t\t\t\t IRQ: "+cpuIRQ+"\n\t\t\t\t\t softIRQ: "+cpuSoftIRQ+"\n\t\t\t\t\t total usage: "+cpuTotal
								+ "\n\t\t\t\t\t usage %: "+cpuUsagePercentage+" %");
						
						datasetRow += ","+cpuUsagePercentage;
						
						if(counter % module == 0 && i==0)
							logger.info("Worker node CPU usage: "+cpuUsagePercentage);
						
						prevIdle[i] = cpuIdle;
						prevNonIdle[i] = cpuNonIdle;
						prevTotal[i] = cpuTotal;
					}
				}
				
				
				logger.debug("Inserting in dataset the following row: "+datasetRow);
				if(!firstRead && cpuDatasetCreated){
					outCpuDataset.write(datasetRow + "\n");
					outCpuDataset.flush();
					readerProcStat.close();
				}
				else firstRead = false;
				
			} catch (Exception e) {
				logger.error("An error occurred", e);
			}
			try {
				logger.debug("Sleeping for " + SchedulerConf.getInstance().getTimeBucketSize() / module + " s...");
				Thread.sleep(SchedulerConf.getInstance().getTimeBucketSize() * 1000 / module);
			} catch (InterruptedException e) {
				logger.error("Error while sleeping", e);
			}
		}
		
	}
	
	
	private String getLastCpuDataset(String[] cpuDatasetFilesList) {
		long timestamp = 0;
		long max = 0;
		String maxString = "";
		for (String s : cpuDatasetFilesList) {			
			try {
				timestamp = Long.parseLong(s.substring(
						cpuDatasetFilePrefix.length(), s.lastIndexOf('.')));
				if (timestamp > max) {
					max = timestamp;
					maxString = s;
				}	
			} catch (Exception e) {
				timestamp = 0;
			}
		}
		logger.error("MAX CPU DATASET FILE TIMESTAMP: " + max);
		return maxString;
	}
	
	/**
	 * @return the total amount of time (in nanoseconds) of all the threads currently alive in this worker
	 */
	private long getTotalCpuTime() {
		long totalCpuTime = 0;
		
		long threadIds[] = ManagementFactory.getThreadMXBean().getAllThreadIds();
		for (int i = 0; i < threadIds.length; i++)
			totalCpuTime += ManagementFactory.getThreadMXBean().getThreadCpuTime(threadIds[i]);
		
		return totalCpuTime;
	}
}
