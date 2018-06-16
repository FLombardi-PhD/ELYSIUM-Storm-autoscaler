package midlab.storm.autoscaling.profiler;

import java.io.PrintWriter;

import midlab.storm.autoscaling.topology.Topology;
import midlab.storm.autoscaling.utility.GraphUtils;

import org.apache.log4j.Logger;

/**
 * class WorkerCpuProfiler
 * @author Federico Lombardi - Sapienza University of Rome
 *
 */
public class WorkerCpuProfiler {

	private static final Logger log4j = Logger.getLogger(WorkerCpuProfiler.class);
	
	/**
	 * Extend the array given as input and return the same array with the first entry void
	 * @param a the input array
	 * @return the extended array
	 */
	public static String[] extendLoadArray(String[] a){
		if (a != null) {
			String[] b = new String[a.length+1];
			for (int i=a.length-1; i>=0; --i) {
				b[i+1] = a[i];
			}
			return b;
		}
		else {
			String[] b = new String[1];
			return b;
		}
	}
	
	/**
	 * Update the workerCpuUsageHistory array by adding new profiled value
	 * @param t
	 * @param workerHostnames
	 * @param iter
	 * @return
	 * @throws Exception
	 */
	public static Topology profileWorkerCPuUsage(Topology t, String[] workerHostnames, int iter, PrintWriter outWorker) throws Exception{
		for (String worker : workerHostnames) {
			log4j.info("Worker "+worker+":");
			
			/*
			 * get sum of 1) input traffic of executors in current worker
			 *            2) output traffic of executors in current worker
			 *            3) load of executors in current worker
			 *            4) cpu usage percentage of worker 
			 */
			int summedExecutorInputTraffic = GraphUtils.getSummedExecutorInputTrafficInWorker(worker);
			int summedExecutorOutputTraffic = GraphUtils.getSummedExecutorOutputTrafficInWorker(worker);
			long summedExecutorLoad = GraphUtils.getSummedExecutorLoadInWorker(worker);
			int workerCpuUsage = GraphUtils.getWorkerCpuUsage(worker);
			
			// create the 4-tuple INPUT_TRAFFIC,OUTPUT_TRAFFIC,LOAD,CPU_USAGE
			String tupleInputOutputLoad = "" + summedExecutorInputTraffic + "," + summedExecutorOutputTraffic + "," + summedExecutorLoad + "," + workerCpuUsage;
			String[] oldWorkerCpuUsageHistory = t.getWorkerCpuUsageHistory();
			String[] newWorkerCpuUsageHistory = extendLoadArray(oldWorkerCpuUsageHistory);
			log4j.info("\tsummed executors input traffic: " + summedExecutorInputTraffic + " tuple/sec");
			log4j.info("\tsummed executors output traffic: " + summedExecutorOutputTraffic + " tuple/sec");
			log4j.info("\tsummed executors load: " + summedExecutorLoad + " Hz");
			log4j.info("\tworker cpu usage: "  +workerCpuUsage + "%");
			
			// update the workerCpuUsageHistory by adding at the top the new tuple
			newWorkerCpuUsageHistory[0] = tupleInputOutputLoad;
			outWorker.println(worker+","+tupleInputOutputLoad);
			outWorker.flush();
			t.setWorkerCpuUsageHistory(newWorkerCpuUsageHistory);
		}
		System.out.println();
		return t;
	}
	
}
