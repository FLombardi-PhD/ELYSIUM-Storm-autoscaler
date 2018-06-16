package midlab.storm.scheduler;

import java.lang.management.ManagementFactory;
import java.util.Map;

import midlab.storm.scheduler.db.DbConf;
import midlab.storm.scheduler.db.NodeManager;

import org.apache.log4j.Logger;

import cpuinfo.CPUInfo;
import backtype.storm.hooks.BaseTaskHook;
import backtype.storm.hooks.info.BoltExecuteInfo;
import backtype.storm.hooks.info.EmitInfo;
import backtype.storm.metric.api.CountMetric;
import backtype.storm.metric.api.MeanReducer;
import backtype.storm.metric.api.MultiCountMetric;
import backtype.storm.metric.api.ReducedMetric;
import backtype.storm.task.TopologyContext;


public class SchedulerHook extends BaseTaskHook {
	
	public static final String TRAFFIC_METRIC = "traffic";
	public static final String LOAD_METRIC = "load";
	public static final String EXECUTE_LATENCY_METRIC = "executeLatency";
	
	/**
	 * one entry for each source task, each entry is the number of tuple 
	 * sent to this thread in the last time bucket
	 */
	private transient MultiCountMetric traffic;
	
	/**
	 * updated at most once every second, it is the number of CPU cycles used 
	 * by this thread during the last time bucket
	 */
	private transient CountMetric load;
	
	/**
	 * average value of execute latency. It has been reseted every second
	 */
	private transient ReducedMetric executeLatency;
	
	/**
	 * fields aimed at limiting the frequency of CPU time checking
	 */
	private long lastLoadUpdate;
	private long lastLoad;
	
	private double timeToCpuCycles;
	
	private boolean isSpout;
	
	public SchedulerHook() {
		
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context) {
		/*
		 * We have at disposal load measurements expressed in time unit, we 
		 * need to convert them in CPU cycles in order to be independent of
		 * CPU speed.
		 * Furthermore, we assume that an executor runs sequentially, so we
		 * have to consider the speed of single core.
		 * We want to measure the load in Hz (s^-1), and time measurements are 
		 * in ns (s*10^-9), so we need to divide by 10^-9.
		 * The unit of this constant is s^-2
		 */
		timeToCpuCycles = ((double)CPUInfo.getInstance().getTotalSpeed() / CPUInfo.getInstance().getNumberOfCores()) / 1000000000L;
		
		DbConf.getInstance().setNimbusHost((String)conf.get("nimbus.host"));
		NodeManager.update();
		
		// SchedulerConf.getInstance().init(conf);
		WorkerManager.init(context.getThisWorkerPort());
		
		// register traffic metric
		traffic = new MultiCountMetric();
		context.registerMetric(TRAFFIC_METRIC, traffic, SchedulerConf.getInstance().getTimeBucketSize());
		
		// register load metric only if this is the beginning task of an executor
		load = new CountMetric();
		context.registerMetric(LOAD_METRIC, load, SchedulerConf.getInstance().getTimeBucketSize());
		
		// register process latency metric
		executeLatency = new ReducedMetric(new MeanReducer());
		context.registerMetric(EXECUTE_LATENCY_METRIC, executeLatency, SchedulerConf.getInstance().getTimeBucketSize());
		
		
		isSpout = context.getRawTopology().get_spouts().get(context.getThisComponentId()) != null;
		
		Logger.getLogger(SchedulerHook.class).info(
			"New SchedulerHook created for task " + context.getThisComponentId() + 
			"[" + context.getThisTaskId() + "], time-to-CPU-cycles: " + timeToCpuCycles + ", is-spout: " + isSpout);
	}
	
	@Override
	public void boltExecute(BoltExecuteInfo info) {
		// Logger.getLogger(SchedulerHook.class).info("boltExecute() task ID: " + info.executingTaskId);
		
		if (info.tuple.getSourceTask() >= 0) {
			// update traffic
			traffic.scope(Integer.toString( info.tuple.getSourceTask() )).incr();
						
			updateLoad();
			
			// update process latency
			executeLatency.update(info.executeLatencyMs);
						
			/* not useful. The reset happens automatically every time bucket 
			 * when the metrics are send to SchedulerMetricsConsumer
			// reset metrics every second
			long t = System.currentTimeMillis();
	        if (t - lastProcessLatencyUpdate >= 1000) {
	        	processLatency.getValueAndReset();
	        	lastProcessLatencyUpdate = t;
	        }
	        */
		}
	}
	
	@Override
	public void emit(EmitInfo info) {
		if (isSpout)
			updateLoad();
	}
	
	private void updateLoad() {
		/*
		 * Update load only once per second, so as to limit the overhead of 
		 * system calls.
		 */
		long t = System.currentTimeMillis();
		if (t - lastLoadUpdate >= 1000) {
			long currentLoad = ManagementFactory.getThreadMXBean().getCurrentThreadCpuTime();
			long increment = (long)(timeToCpuCycles * (currentLoad - lastLoad)); // CPU cycles
			load.incrBy(increment);
			lastLoadUpdate = t;
			lastLoad = currentLoad;
		}
	}
	
}
