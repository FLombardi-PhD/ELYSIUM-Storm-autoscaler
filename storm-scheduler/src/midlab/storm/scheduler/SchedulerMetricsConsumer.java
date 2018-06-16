package midlab.storm.scheduler;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import midlab.storm.scheduler.db.DbConf;
import midlab.storm.scheduler.db.CommonDataManager;

import org.apache.log4j.Logger;

import backtype.storm.metric.api.IMetricsConsumer;
import backtype.storm.task.IErrorReporter;
import backtype.storm.task.TopologyContext;

public class SchedulerMetricsConsumer implements IMetricsConsumer {
	
	private Logger logger;
	
	private String topologyId;
	
	private String executeLatencyDirName;
	private Map<Integer, PrintWriter> executeLatencyWriterMap;
	
	// private int timeWindowLength = SchedulerConf.getInstance().getTimeBucketNumber() * SchedulerConf.getInstance().getTimeBucketSize(); // seconds
	
	/**
	 * sourceTaskID -> destinationTaskID -> list of traffic info (number of tuples in a time bucket) for the last K time buckets
	 */
	private Map<Integer, Map<Integer, List<Integer>>> trafficMap;
	
	/**
	 * taskID -> list of load info (CPU cycles in a time bucket) for the last K time buckets
	 */
	private Map<Integer, List<Long>> loadMap;
	
	private Map<String, String> unexpectedMetricMap;

	@Override
    public void prepare(@SuppressWarnings("rawtypes") Map stormConf, Object registrationArgument, TopologyContext context, IErrorReporter errorReporter) {
		logger = Logger.getLogger(SchedulerMetricsConsumer.class);
		logger.info("prepare()");
		DbConf.getInstance().setNimbusHost((String)stormConf.get("nimbus.host"));
		
		topologyId = context.getStormId();
		trafficMap = new HashMap<Integer,Map<Integer,List<Integer>>>();
		loadMap = new HashMap<Integer,List<Long>>();
		unexpectedMetricMap = new HashMap<String, String>();
		
		// SchedulerConf.getInstance().init(stormConf);
		
		/*logger.info("Provided configuration:");
		logger.info("========================================");
		for (Object key : stormConf.keySet()) {
			Object value = stormConf.get(key);
			logger.info("- " + key + ": " + value);
		}
		logger.info("========================================");*/
		
		executeLatencyDirName = "execute_latency."+topologyId;
		File executeLatencyDir = new File(executeLatencyDirName);
		boolean success = executeLatencyDir.mkdir();
		if (success) {
			logger.info("Execute latency directory "+executeLatencyDirName+" created succesfully.");
			executeLatencyWriterMap = new HashMap<Integer, PrintWriter>();
			Set<String> componentIds = context.getComponentIds();
			for (String componentId : componentIds) {
				logger.info("Founded component "+componentId);
				List<Integer> taskIds = context.getComponentTasks(componentId);
				for (Integer taskId : taskIds) {
					logger.info("Founded task "+taskId+" related to component "+componentId);
					File executeLatencyFile = new File(executeLatencyDir, "execute_latency."+topologyId+"."+taskId+".csv");
					try {
						executeLatencyWriterMap.put(taskId, new PrintWriter(executeLatencyFile));
						logger.info("Execute latency writer for task "+taskId+" added to map succesfully.");
					} catch (FileNotFoundException e) {
						logger.error("An error occured while creating latency writer", e);
					}
				}
			}
		}
		else {
			logger.info("Execute latency directory "+executeLatencyDirName +" yet exist.");
		}
		
		
	}
	
	@SuppressWarnings("unchecked")
	@Override
    public void handleDataPoints(TaskInfo taskInfo, Collection<DataPoint> dataPoints) {
		logger.info("handleDataPoints() - BEGIN");
		logger.info("Task Info: " + taskInfo.srcComponentId + "[" + taskInfo.srcTaskId + "]@" + taskInfo.srcWorkerHost + ":" + taskInfo.srcWorkerPort);
		for (DataPoint dp : dataPoints) {
			logger.info("Data Point: " + dp + " (" + dp.value.getClass() + ")");
			if (dp.name.equals(SchedulerHook.LOAD_METRIC)) {
				if (taskInfo.srcTaskId >= 0)
					updateLoad(taskInfo.srcTaskId, (long)dp.value, taskInfo.srcWorkerHost, taskInfo.srcWorkerPort);
			} else if (dp.name.equals(SchedulerHook.TRAFFIC_METRIC)) {
				updateTraffic(taskInfo.srcTaskId /* this actually is the destination task!!! */, (Map<String, Long>)dp.value);
			} else if (dp.name.equals(SchedulerHook.EXECUTE_LATENCY_METRIC)) {
				/*
				 * TODO da gestire: crea problemi quando facciamo rebalance perchè rialloca l'executor
				 * se un altro nodo e dovrei ricreare il file
				 * 
				 * updateExecuteLatency(taskInfo.srcTaskId, (double)dp.value);
				 */
			}			
			else {
				if (!unexpectedMetricMap.containsKey(dp.name)) {
					logger.warn("Unexpected metric: " + dp.name);
					unexpectedMetricMap.put(dp.name, dp.name);
				}
			}
		}
		logger.info("handleDataPoints() - END");
	}
	
	private void updateLoad(int taskId, long load, String workerHostname, int workerPort) {
		
		// update local state
		List<Long> loadList = loadMap.get(taskId);
		if (loadList == null) {
			loadList = new ArrayList<Long>();
			loadMap.put(taskId, loadList);
		}
		loadList.add((long)load);
		if (loadList.size() > SchedulerConf.getInstance().getTimeBucketNumber())
			loadList.remove(0);
		
		// compute new load
		long totalLoad = 0;
		int timeWindowLength = 0;
		for (long l : loadList) {
			totalLoad += l;
			timeWindowLength += SchedulerConf.getInstance().getTimeBucketSize();
		}
		long newLoad = totalLoad / timeWindowLength;
		
		// store load
		try {
			CommonDataManager.getInstance().updateLoad(topologyId, taskId, newLoad, workerHostname, workerPort);
		} catch (Exception e) {
			logger.error("Error updating load", e);
		}
	}
	
	/**
	 * @param dstTaskId
	 * @param taskDeltaTrafficMap srcTaskID -> tuples sent by src task to dst task during the last time bucket
	 */
	private void updateTraffic(int dstTaskId, Map<String, Long> taskDeltaTrafficMap) {
		for (String srcTaskIdStr : taskDeltaTrafficMap.keySet()) {
			// extract info from params (srcTaskId -> dstTaskId: traffic)
			int srcTaskId = Integer.parseInt(srcTaskIdStr);
			long traffic = taskDeltaTrafficMap.get(srcTaskIdStr);
			
			// integrate extracted info to local state
			Map<Integer, List<Integer>> srcTaskTrafficMap = trafficMap.get(srcTaskIdStr); // traffic from srcTask
			if (srcTaskTrafficMap == null) {
				srcTaskTrafficMap = new HashMap<Integer, List<Integer>>();
				trafficMap.put(srcTaskId, srcTaskTrafficMap);
			}
			List<Integer> srcToDstTrafficList = srcTaskTrafficMap.get(dstTaskId); // traffic from srcTask to dstTask
			if (srcToDstTrafficList == null) {
				srcToDstTrafficList = new ArrayList<Integer>();
				srcTaskTrafficMap.put(dstTaskId, srcToDstTrafficList);
			}
			srcToDstTrafficList.add((int)traffic); // update traffic
			if (srcToDstTrafficList.size() > SchedulerConf.getInstance().getTimeBucketNumber())
				srcToDstTrafficList.remove(0);
			
			// compute new traffic
			int totalTraffic = 0;
			int timeWindowLength = 0;
			for (int t : srcToDstTrafficList) {
				totalTraffic += t;
				timeWindowLength += SchedulerConf.getInstance().getTimeBucketSize();
			}
			int newTraffic = totalTraffic / timeWindowLength;
			
			// store traffic
			try {
				CommonDataManager.getInstance().updateTraffic(topologyId, srcTaskId, dstTaskId, newTraffic);
			} catch (Exception e) {
				logger.error("Error updating traffic", e);
			}
		}
	}
	
	/**
	 * @param taskId
	 * @param executeLatency
	 */
	private void updateExecuteLatency(int taskId, double executeLatency){
		PrintWriter executeLatencyWriter = executeLatencyWriterMap.get(taskId);
		if (executeLatencyWriter != null) {
			executeLatencyWriter.println(System.currentTimeMillis()+"\t"+taskId+"\t"+executeLatency);
			executeLatencyWriter.flush();
		}
		else 
			logger.info("Cannot update execute_latency for task "+taskId);
	}
	
	@Override
    public void cleanup() { }
}
