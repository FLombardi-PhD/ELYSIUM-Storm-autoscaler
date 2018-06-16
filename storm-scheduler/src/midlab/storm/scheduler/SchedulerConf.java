package midlab.storm.scheduler;

import java.util.Map;

import org.apache.log4j.Logger;


public class SchedulerConf {
	
	public static final String DEFAULT_MIDLAB_SCHEDULER = SimpleHeuristicScheduler.class.getName();
	public static final int DEFAULT_TIME_BUCKET_NUMBER = 6;
	public static final int DEFAULT_TIME_BUCKET_SIZE = 10; // seconds
	public static final double DEFAULT_ALFA = .5;
	public static final double DEFAULT_BETA = .5;
	public static final int DEFAULT_MIN_TIME_TO_RESCHEDULE = 180; // seconds
	public static final double DEFAULT_WEIGHT_THRESHOLD_TO_RESCHEDULE = .8;
	public static final boolean DEFAULT_LOG_METRICS_TO_FILES = false;
	
	public static final String MIDLAB_SCHEDULER_PARAM = "midlab.scheduler";
	public static final String TIME_BUCKET_NUMBER_PARAM = "midlab.scheduler.time.bucket.number";
	public static final String TIME_BUCKET_SIZE_PARAM = "midlab.scheduler.time.bucket.size";
	public static final String ALFA_PARAM = "midlab.scheduler.alfa";
	public static final String BETA_PARAM = "midlab.scheduler.beta";
	public static final String MIN_TIME_TO_RESCHEDULE_PARAM = "midlab.scheduler.min.time.to.reschedule";
	public static final String WEIGHT_THRESHOLD_TO_RESCHEDULE_PARAM = "midlab.scheduler.weight.threshold.to.reschedule";
	public static final String LOG_METRICS_TO_FILES_PARAM = "midlab.scheduler.log.metrics.to.files";
	
	private String midlabScheduler;
	private int timeBucketNumber;
	private int timeBucketSize;
	private double alfa;
	private double beta;
	private int minTimeToReschedule;
	private double weightThresholdToReschedule;
	private boolean logMetricsToFiles;
	
	private static SchedulerConf instance = null;
	
	private Logger logger;
	
	public static SchedulerConf getInstance() {
		if (instance == null)
			instance = new SchedulerConf();
		return instance;
	}
	
	public void init(@SuppressWarnings("rawtypes") Map conf) {
		
		logger.info("Initializing Scheduler Configuration...");
		
		midlabScheduler = (String)conf.get(MIDLAB_SCHEDULER_PARAM);
		
		try {timeBucketNumber = Integer.parseInt(conf.get(TIME_BUCKET_NUMBER_PARAM).toString()); }
		catch(Exception e) { logger.error("Error parsing parameter " + TIME_BUCKET_NUMBER_PARAM, e);}
		
		try {timeBucketSize = Integer.parseInt(conf.get(TIME_BUCKET_SIZE_PARAM).toString()); }
		catch(Exception e) { logger.error("Error parsing parameter " + TIME_BUCKET_SIZE_PARAM, e);}
		
		try {alfa = Double.parseDouble(conf.get(ALFA_PARAM).toString()); }
		catch(Exception e) { logger.error("Error parsing parameter " + ALFA_PARAM, e);}
		
		try {beta = Double.parseDouble(conf.get(BETA_PARAM).toString()); }
		catch(Exception e) { logger.error("Error parsing parameter " + BETA_PARAM, e);}
		
		try {minTimeToReschedule = Integer.parseInt(conf.get(MIN_TIME_TO_RESCHEDULE_PARAM).toString()); }
		catch(Exception e) { logger.error("Error parsing parameter " + MIN_TIME_TO_RESCHEDULE_PARAM, e);}
		
		try {weightThresholdToReschedule = Double.parseDouble(conf.get(WEIGHT_THRESHOLD_TO_RESCHEDULE_PARAM).toString()); }
		catch(Exception e) { logger.error("Error parsing parameter " + WEIGHT_THRESHOLD_TO_RESCHEDULE_PARAM, e);}
		
		try {logMetricsToFiles = Boolean.parseBoolean(conf.get(LOG_METRICS_TO_FILES_PARAM).toString()); }
		catch(Exception e) { logger.error("Error parsing parameter " + LOG_METRICS_TO_FILES_PARAM, e);}
		
		logger.info("Scheduler Configuration completed");
	}

	private SchedulerConf() {
		midlabScheduler = DEFAULT_MIDLAB_SCHEDULER;
		timeBucketNumber = DEFAULT_TIME_BUCKET_NUMBER;
		timeBucketSize = DEFAULT_TIME_BUCKET_SIZE;
		alfa = DEFAULT_ALFA;
		beta = DEFAULT_BETA;
		minTimeToReschedule = DEFAULT_MIN_TIME_TO_RESCHEDULE;
		weightThresholdToReschedule = DEFAULT_WEIGHT_THRESHOLD_TO_RESCHEDULE;
		logMetricsToFiles = DEFAULT_LOG_METRICS_TO_FILES;
		logger = Logger.getLogger(SchedulerConf.class);
	}
	
	public String getMidlabSchedulerClass() {
		return midlabScheduler;
	}

	public int getTimeBucketNumber() {
		return timeBucketNumber;
	}

	public int getTimeBucketSize() {
		return timeBucketSize;
	}

	public double getAlfa() {
		return alfa;
	}
	
	public double getBeta() {
		return beta;
	}
	
	public int getMinTimeToReschedule() {
		return minTimeToReschedule;
	}
	
	public double getWeightThresholdToReschedule() {
		return weightThresholdToReschedule;
	}
	
	public boolean logMetricsToFiles() {
		return logMetricsToFiles;
	}
}
