package midlab.storm.scheduler;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import midlab.storm.autoscaling.forecasting.Forecaster;
import midlab.storm.autoscaling.topology.Component;
import midlab.storm.scheduler.data.Allocation;
import midlab.storm.scheduler.data.Executor;
import midlab.storm.scheduler.data.Node;
import midlab.storm.scheduler.data.StormCluster;
import midlab.storm.scheduler.data.Topology;
import midlab.storm.scheduler.db.DbServer;
import midlab.storm.scheduler.db.SchedulerDataManager;

import org.apache.log4j.Logger;

import backtype.storm.generated.Nimbus.Client;
import backtype.storm.generated.RebalanceOptions;
import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.EvenScheduler;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.IScheduler;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;

public class AutoScaler implements IScheduler {
	
	private static final String DEFAULT_STORM_COMMAND_DIR = "/home/midlab/apache-storm-0.9.1-incubating/bin";
	private static final String DEFAULT_STORM_DATA_DIR = "/home/midlab/apache-storm-0.9.1-incubating/forecaster";
	private static final int DEFAULT_FORECAST_HORIZON = 10;
	private static final int DEFAULT_TUPLE_REPLICATION_FACTOR = 10;
	private static final int DEFAULT_TEMPORAL_OFFSET = 0;
	private static final int DEFAULT_START_AFTER = 900;
	private static final boolean DEFAULT_PROACTIVE = true;
	private static final boolean DEFAULT_SCALE_MACHINES_PARAM = true;
	private static final boolean DEFAULT_SCALE_OPERATORS_PARAM = true;
	
	private static final String STORM_COMMAND_DIR_PARAM = "midlab.autoscaler.storm.command.dir";
	private static final String STORM_DATA_DIR_PARAM = "midlab.autoscaler.storm.data.dir";
	private static final String FORECAST_HORIZON_PARAM = "midlab.autoscaler.forecast.horizon";
	private static final String TUPLE_REPLICATION_FACTOR_PARAM = "midlab.autoscaler.tuple.replication.factor";
	private static final String TEMPORAL_OFFSET_PARAM = "midlab.autoscaler.temporal.offset";
	private static final String START_AFTER_PARAM = "midlab.autoscaler.start.after";
	private static final String PROACTIVE_PARAM = "midlab.autoscaler.proactive";
	private static final String SCALE_MACHINES_PARAM = "midlab.autoscaler.scale.machines";
	private static final String SCALE_OPERATORS_PARAM = "midlab.autoscaler.scale.operators";
	private static final String REBALANCE_ALWAYS = "midlab.autoscaler.rebalance.always";
	private static final int REBALANCE_WAIT = 1;
	
	private Logger logger;
	
	private String stormCommandDir;
	private String stormDataDir;
	
	private int autoScalerIteration;
	private int startAfter;
	private boolean forecasterStarted;
	
	/**
	 * measured in minutes
	 */
	private int forecastHorizon;
	
	/**
	 * [!!! synch with the topology !!!]
	 */
	private int tupleReplicationFactor;
	
	/**
	 * measured in seconds [!!! synch with the data driver !!!]
	 */
	private int temporalOffset;
	
	/**
	 * flag to say if the AutoScaler has to be proactive (true) or reactive (false)
	 */
	private boolean proactive;
	
	/**
	 * flag to say if the autoscaler has to scale on machines or not
	 */
	private boolean scaleMachines;
	
	/**
	 * flag to say if the autoscaler has to scale the operators or not
	 */
	private boolean scaleOperators;
	
	/**
	 * flag to call rebalance always even if no modify are done
	 */
	private boolean rebalanceAlways;
	/**
	 * timestamp of the last check on resource provisioning
	 */
	private long lastAssessment;
	
	/**
	 * timestamp of the last time metrics have been read from DB
	 */
	private long lastMetricUpdate;
	
	private StormCluster stormCluster;
	
	private Forecaster forecaster;
	
	// TODO new!!!!
	private Map<String, Map<String, Integer>> componentToParallelism;
	
	private File nodeHistory;
	private File executorHistory;
	private PrintWriter outNodeHistory;
	private PrintWriter outExecutorHistory;
	
	private int readIntParam(@SuppressWarnings("rawtypes") Map conf, String param, int defaultValue) {
		int value = defaultValue;
		try {
			value = Integer.parseInt(conf.get(param).toString());
		} catch (Exception e) {
			logger.error("Error reading " + param + " param: " + conf.get(param) + "; set default value " + defaultValue, e);
		}
		return value;
	}
	
	private String readStringParam(@SuppressWarnings("rawtypes") Map conf, String param, String defaultValue) {
		String value = defaultValue;
		try {
			value = conf.get(param).toString();
		} catch (Exception e) {
			logger.error("Error reading " + param + " param: " + conf.get(param) + "; set default value " + defaultValue, e);
		}
		return value;
	}
	
	private boolean readBooleanParam(@SuppressWarnings("rawtypes") Map conf, String param, boolean defaultValue) {
		boolean value = defaultValue;
		String string;
		try {
			string = conf.get(param).toString();
			if (!string.equalsIgnoreCase("true") && !string.equalsIgnoreCase("false"))
				throw new Exception();
			value = Boolean.valueOf(string);
		} catch (Exception e) {
			logger.error("Error reading " + param + " param: " + conf.get(param) + "; set default value " + defaultValue, e);
		}
		return value;
	}
	
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map conf) {
		logger = Logger.getLogger(AutoScaler.class);
		logger.info("Preparing AutoScaler...");
		
		// start the embedded server
		DbServer.getInstance();
		
		// init the SchedulerDataManager to ensure tables are created before any other process accesses them
		SchedulerDataManager.getInstance();
		
		// Storm command dir
		stormCommandDir = readStringParam(conf, STORM_COMMAND_DIR_PARAM, DEFAULT_STORM_COMMAND_DIR);
		logger.info("Storm Command Dir: " + stormCommandDir);
		
		// Storm data dir
		stormDataDir = readStringParam(conf, STORM_DATA_DIR_PARAM, DEFAULT_STORM_DATA_DIR);
		logger.info("Storm Data Dir: " + stormDataDir);
		
		// forecast horizon
		forecastHorizon = readIntParam(conf, FORECAST_HORIZON_PARAM, DEFAULT_FORECAST_HORIZON);
		logger.info("Forecast Horizon: " + forecastHorizon + " minutes");
		
		// tuple replication factor
		tupleReplicationFactor = readIntParam(conf, TUPLE_REPLICATION_FACTOR_PARAM, DEFAULT_TUPLE_REPLICATION_FACTOR);
		logger.info("Tuple Replication Factor: " + tupleReplicationFactor);
		
		// temporal offset
		temporalOffset = readIntParam(conf, TEMPORAL_OFFSET_PARAM, DEFAULT_TEMPORAL_OFFSET);
		logger.info("Temporal Offset: " + temporalOffset + " seconds");
		
		startAfter = readIntParam(conf, START_AFTER_PARAM, DEFAULT_START_AFTER);
		logger.info("Start Scaling after: " + startAfter + " iterations");
		
		// proactive AutoScaler
		proactive = readBooleanParam(conf, PROACTIVE_PARAM, DEFAULT_PROACTIVE);
		logger.info("Proactive: " + proactive);
		
		scaleMachines = readBooleanParam(conf, SCALE_MACHINES_PARAM, DEFAULT_SCALE_MACHINES_PARAM);
		logger.info("Scale machines: " + scaleMachines);
		
		scaleOperators = readBooleanParam(conf, SCALE_OPERATORS_PARAM, DEFAULT_SCALE_OPERATORS_PARAM);
		logger.info("Scale operators: " + scaleOperators);
		
		rebalanceAlways = readBooleanParam(conf, REBALANCE_ALWAYS, false);
		
		try {
			forecaster = new Forecaster(stormDataDir, temporalOffset, tupleReplicationFactor, proactive, forecastHorizon, rebalanceAlways);
			logger.info("Forecaster correctly instantiated");
		} catch (Exception e) {
			logger.error("An error occurred creating the forecaster", e);
			throw new RuntimeException(e);
		}
		
		// <topologyName, <componentName, parallelism>>
		componentToParallelism = new HashMap<String, Map<String, Integer>>();
		
		// prepare output file on which printing the number of nodes on each assessment
		long time = System.currentTimeMillis();
		nodeHistory = new File("node_history." + time + ".csv");
		executorHistory = new File("executor_history." + time + ".csv");
		try {
			outNodeHistory = new PrintWriter(nodeHistory);
			outExecutorHistory = new PrintWriter(executorHistory);
		} catch (FileNotFoundException e) {
			logger.error("Impossible to create node or executor history file writer.", e);
		}
		
		// set the first autoscaler iteration
		autoScalerIteration = 0;
		forecasterStarted = false;
	}

	@Override
	public void schedule(Topologies topologies, Cluster cluster) {
		
		// start autoscaler
		long now = System.currentTimeMillis();
		logger.info("+++++++++++++++++++++");
		logger.info("Begin AutoScaler (instance " + this + ")");
				
		
		// checking single deployed topology		
		String forecasterTopologyName = "";
		if (topologies.getTopologies().size() > 1)
			logger.warn("Found more than one topology, the autoscaler currently works for a single topology only");
		for (TopologyDetails t : topologies.getTopologies()) {
			forecasterTopologyName = t.getName();
			logger.info("Considering topology " + forecasterTopologyName);
			
			if (!forecasterStarted) {				
				// creo una mappa <ComponentName, <list task>>
				Hashtable<String, ArrayList<Integer>> componentToTaskList = new Hashtable<String, ArrayList<Integer>>();
				for (String componentName : t.getExecutorToComponent().values()) {
					// creo la entry con la relativa lista di task vuota
					componentToTaskList.put(componentName, new ArrayList<Integer>());
				}
				for (ExecutorDetails ed : t.getExecutors()) {
					// per ogni executor details mappo i task
					String component = t.getExecutorToComponent().get(ed);
					int startTask = ed.getStartTask();
					int endTask = ed.getEndTask();
					for (; startTask <= endTask; ++startTask) {
						componentToTaskList.get(component).add(startTask);
						logger.info("Added to " + component +" task " + startTask);
					}
				}
				forecaster.assignRealTaskNumber(componentToTaskList);
				forecasterStarted = true;
			}
			break; // to consider only one topology!
		}
		
		
		// check assignable and used slots
		if (autoScalerIteration == 0) {			
			StormCluster startingStormCluster;
			try {
				startingStormCluster = SchedulerDataManager.getInstance().loadCluster(cluster);
				if (startingStormCluster == null) {
					Thread.sleep(1000);
					return;
				}
				else {
					forecaster.setStormCluster(startingStormCluster);
					logger.info("StormCluster set succesfully.");
					++autoScalerIteration;
				}
			} catch (Exception e) {
				logger.error("Some error occur while loading data of nodes.", e);
			}
		}
		
		
		// set current configuration
		Collection<WorkerSlot> workerSlotsUsed = cluster.getUsedSlots();
		List<String> workerSlotUsedString = new ArrayList<String>();
		for (WorkerSlot workerSlot : workerSlotsUsed) {
			workerSlotUsedString.add(workerSlot.getNodeId());
		}
		forecaster.setCurrentConfig(workerSlotUsedString);
		
		
		// clean DB
		List<String> runningTopologyList = new ArrayList<String>();
		for (TopologyDetails topology : topologies.getTopologies())
			runningTopologyList.add(topology.getId());
		try {
			SchedulerDataManager.getInstance().cleanDB(runningTopologyList);
		} catch (Exception e) {
			// in case of error, go on
			logger.error("Cannot clean DB", e);
		}
		
		
		// TODO spostare dentro il primo if sotto
		// check for reading updated metrics from DB
		if (lastMetricUpdate == 0 || now - lastMetricUpdate >= 60 * 1000) {
			// invoke update method
			try {
				forecaster.setCurrentTraffic();
				forecaster.setCurrentWorkerCpuUsage();
				logger.info("Updated metrics read from DB");
				lastMetricUpdate = now;
			} catch (Exception e) {
				logger.error("An error occurred in the Forecaster while reading metrics from DB", e);
			}
		}
		
		
		// check for doing a new assessment of the configuration
		if (lastAssessment == 0 || now - lastAssessment >= forecastHorizon * 1000 * 60) {
			
			
			
			if (autoScalerIteration >= startAfter) {
				// check for a better configuration

				// load storm cluster info
				if (stormCluster == null) {
					try {
						stormCluster = SchedulerDataManager.getInstance().loadCluster(cluster);
						logger.info("Storm cluster data loaded");
					} catch (Exception e) {
						logger.error("Error loading cluster info from DB", e);
					}
				}

				// load topologies info
				Map<String, Topology> topologyMap = null;
				try {
					topologyMap = SchedulerDataManager.getInstance().loadData(topologies);
					logger.info("Topology info loaded");
				} catch (Exception e) {
					logger.error("Error loading topology stats from DB", e);
				}

				// set map  <topologyName, <componentName, parallelism>>
				for (Topology t : topologyMap.values()) {
					if (!t.getName().equals(forecasterTopologyName)) {
						logger.warn("The topology " + t.getName() + " is different from " + forecasterTopologyName);
						continue;
					}
					
					logger.info("Reading component to parallelism map of " + t.getName());
					Map<String, Integer> currentTopologyParallelismMap = new HashMap<String, Integer>();
					int currentParallelism;
					for (Executor e : t.getExecutors()) {
						String componentName = e.getKey().getComponentId();
						logger.debug("\t- component: " + componentName);
						if (currentTopologyParallelismMap.containsKey(componentName)) {
							currentParallelism = currentTopologyParallelismMap.get(componentName);
							currentParallelism++;
							currentTopologyParallelismMap.put(componentName, currentParallelism);
							logger.debug("\t\t- paral: " + currentParallelism);
						}
						else {
							currentParallelism = 0;
							currentParallelism++;
							currentTopologyParallelismMap.put(componentName, currentParallelism);
							logger.debug("\t\t- paral: " + currentParallelism);
						}
					}
					componentToParallelism.put(t.getName(), currentTopologyParallelismMap);
				}
				
				
				if (stormCluster != null && topologyMap != null	&& !topologyMap.isEmpty()) {
					
					if (cluster.needsSchedulingTopologies(topologies).isEmpty()) {

						// compute prediction
						boolean predictionComputed = true;
						try {
							logger.info("Invoking Forecaster..");
							forecaster.start(componentToParallelism.get(forecasterTopologyName));
						} catch (Exception e) {
							logger.error("Error computing predition", e);
							predictionComputed = false;
						}

						if (predictionComputed) {
							
							/** spostato qui **/
							// check whether such best configuration is different from the current one
							Topology topology = null;
							for (Topology t : topologyMap.values()) {
								topology = t;
								break;
							}
							int currentNodeCount = topology.getWorkerCount();
							/****/
							
							
							
							/** PART1: setting operator parallelism **/				
							
							if (scaleOperators || rebalanceAlways) {
								
								boolean ok = true;
			
								// create the new object topologies and update all topologies (currently support only one)
								Map<String, TopologyDetails> newTopologiesMap = new HashMap<String, TopologyDetails>();
								try{
									for (TopologyDetails td : topologies.getTopologies()) {	
									
										logger.info("Building new executor to component map for " + td.getName() + " (id: " + td.getId() + ")");
									
										// build the new map <ExecutorDetails,Component>
										Map<ExecutorDetails, String> newExecutorToComponentMap = simulateNewTaskToExecutorMapping(forecaster.getTopology().getComponents(), td);
																		
										// build the new TopologyDetails with the info of new executors
										TopologyDetails newTd = new TopologyDetails(td.getId(), td.getConf(), td.getTopology(), 4, newExecutorToComponentMap);
										Topology newTopo = new Topology(newTd);
										
										// update TopologyMap with the new Topology and insert into newTopologiesMap
										topologyMap.put(newTopo.getId(), newTopo);
										newTopologiesMap.put(newTopo.getId(), newTd);
									}
								} catch (Exception e) {
									ok = false;
									logger.error("Error changing operator parallelism.", e);
								} finally {
									// update topologies with the newTopologiesMap
									if (ok)
										topologies = new Topologies(newTopologiesMap);
								}
															
								// print the new executor map
								logger.info("New executor map:");
								for (String s : topologyMap.keySet()) {
									Collection<Executor> execs = topologyMap.get(s).getExecutors();
									for (Executor e : execs) {
										logger.info("\t- " + s + "; " + e.toString() + "; " + e.getStormExecutor().toString());	
									}
									logger.info("worker for topology " + topologyMap.get(s).getWorkerCount());
								}
							} /* end if scaleOperators */
														
							
							
							/** PART2: setting the number of worker nodes **/
							
							
							
							logger.info("current node count: " + currentNodeCount);
							int nodeCount;
							
							if (!scaleMachines) {
								nodeCount = currentNodeCount;
								logger.info("AutoScaler does not change number of nodes: keep " + nodeCount + " nodes.");
							}
							else {
								// iterate through the number of nodes
								nodeCount = 1;
								Node nodeArray[] = new Node[stormCluster.getNodes().size()];
								stormCluster.getNodes().toArray(nodeArray);
								MidlabEvenScheduler scheduler = new MidlabEvenScheduler();
								for (; nodeCount <= nodeArray.length; nodeCount++) {

									// create a storm cluster with nodeCount nodes
									List<Node> nodes = new ArrayList<Node>();
									for (int i = 0; i < nodeCount; i++)
										nodes.add(nodeArray[i]);
									StormCluster tmpStormCluster = new StormCluster(nodes);
									logger.info("Try this configuration with "
											+ nodeCount + " nodes: "
											+ tmpStormCluster);

									// compute an allocation according to the evenscheduler
									Allocation tmpAllocation = null;
									try {
										tmpAllocation = scheduler.schedule(tmpStormCluster, topologyMap);
										logger.info("Resulting allocation: " + tmpAllocation);
									} catch (Exception e) {
										logger.error("An error occurred computing an allocation with " + nodeCount + " nodes", e);
									}

									if (tmpAllocation != null) {
										// invoke method for detecting possible bottleneck;
										// if no bottleneck is detected, then break
										try {
											if (forecaster.isMinimumAllocation(tmpStormCluster, tmpAllocation))
												break;
										} catch (Exception e) {
											logger.error("An error occurred checking whether the allocation with "
															+ nodeCount + " nodes can sustain forecasted traffic", e);
										}
									}

								} /* end for cycle */

								
								if (nodeCount > nodeArray.length) {
									logger.info("By even using all available resources, bottlenecks cannot be avoided; use all the resources anyway");
									nodeCount = nodeArray.length;
								}
								
								logger.info("Current configuration uses "
										+ currentNodeCount
										+ " nodes, best configuration needs "
										+ nodeCount + " nodes");
								
							} /* end else condition on scaleMachines */
							
														
							
							/** PART3: set the rebalance options and rebalance **/
													
							
							
							logger.info("Scaling Machines:  " + scaleMachines);
							logger.info("Scaling Operators: " + scaleOperators);
							
							
							// prepare the output node and executor files
							long time = System.currentTimeMillis();
							String executorString = time + "";
														
							// set executors (if any) that need to be rebalanced
							boolean differentOpearatorParallelism = false;
							Map<String,Integer> num_executors = new HashMap<String,Integer>();
							if (scaleOperators || rebalanceAlways) {
								for (Component c : forecaster.getTopology().getComponents()) {										
									if (forecaster.getActualParallelism().containsKey(c)) {
										if (c.getParallelism() != forecaster.getActualParallelism().get(c)) {
											num_executors.put(c.getName(), c.getParallelism());
											differentOpearatorParallelism = true;
										}
										executorString += "," + c.getName() + ":" + c.getParallelism();
									}
								}
							}
							
							// print on files
							outExecutorHistory.println(executorString);
							outExecutorHistory.flush();
							outNodeHistory.println(time + "," + nodeCount);
							outNodeHistory.flush();
							
							// if the number of worker should be changed or executors need to be scaled, rebalance
							if (nodeCount != currentNodeCount || differentOpearatorParallelism || rebalanceAlways) {
								RebalanceOptions rebalanceOptions = new RebalanceOptions();
								
								if (nodeCount != currentNodeCount)
									rebalanceOptions.set_num_workers(nodeCount);
								
								if (differentOpearatorParallelism)
									rebalanceOptions.set_num_executors(num_executors);
								
								rebalanceOptions.set_wait_secs(REBALANCE_WAIT);
								logger.info("Detaching a thread to rebalance.");
								executeRebalance(topology.getName(), rebalanceOptions);
							}

							lastAssessment = now;
							
						} else {
							logger.info("Cannot compute prediction, skip any assessment for now");
						}
					} else {
						logger.info("There are topologies needing rescheduling, skip any assessment for now");
					}
					
					forecasterStarted = true;
					
				} else {
					logger.info("Required info on the cluster or the topology are missing yet, skip any assessment for now");
				}
			}
			else {
				logger.info("Current iteration is " + autoScalerIteration + ". Wait iteration " + startAfter + "to start autoscaling.");
			}
			++autoScalerIteration;
		} else {
			// wait
			long elapsedTime = now - lastAssessment;
			long timeToNextAssessment = forecastHorizon * 1000 * 60 - elapsedTime;
			logger.info("Wait for next assessment, " + Math.round(((float)elapsedTime)/1000) + " seconds passed, wait for other " + Math.round((float)timeToNextAssessment/1000) + " seconds");
		}

		logger.info("Applying EvenScheduler...");
		new EvenScheduler().schedule(topologies, cluster);
		logger.info("Ok, EvenScheduler applied");
		logger.info("---------------------");
	}

	
	/**
	 * Simulate the new mapping between Executor and Component
	 * @param components
	 * @param topologyDetails
	 * @return
	 */
	private Map<ExecutorDetails, String> simulateNewTaskToExecutorMapping(Set<Component> components, TopologyDetails topologyDetails) {
		
		Collection<ExecutorDetails> executorsDetails = topologyDetails.getExecutors();
		
		// map to return
		Map<ExecutorDetails, String> newExecutorToComponentMap = new HashMap<ExecutorDetails, String>(0);
		
		// map to associate to each component (i) the list of executors, (ii) the min task and (iii) the max task
		Map<Component, List<ExecutorDetails>> executorToComponentMap = new HashMap<Component, List<ExecutorDetails>>();
		Map<Component, Integer> componentMinTask = new HashMap<Component, Integer>();
		Map<Component, Integer> componentMaxTask = new HashMap<Component, Integer>();
	
		// compute mapping
		int minStartTaskId = Integer.MAX_VALUE;
		int maxEndTaskId = -1;
		int tmp;
		for (Component c : components) {
			
			// scan all component's tasks
			ArrayList<Integer> tasks = c.getTasks();
			for (Integer task : tasks) {
				tmp = task;
				if (tmp < minStartTaskId)
					minStartTaskId = tmp;
				if (tmp > maxEndTaskId)
					maxEndTaskId = tmp;
			}
			
			// associate min and max task to the component
			logger.debug("Associating min & max task to " + c.getName() + ": " + minStartTaskId + ", " + maxEndTaskId);
			componentMinTask.put(c, minStartTaskId);
			componentMaxTask.put(c, maxEndTaskId);
			
			// associate ExecutorDetails with the current component
			int count = 0;
			List<ExecutorDetails> executorDetailsList;
			for (ExecutorDetails ed : executorsDetails) {
				if (ed.getStartTask() >= minStartTaskId && ed.getEndTask() <= maxEndTaskId) {
					if (!executorToComponentMap.containsKey(c))
						executorDetailsList = new ArrayList<ExecutorDetails>();
					else
						executorDetailsList = executorToComponentMap.get(c);
					executorDetailsList.add(ed);
					executorToComponentMap.put(c, executorDetailsList);
					++count;
				}
			}
			logger.info("Associated " + count + " executors to " + c.getName());
			minStartTaskId = Integer.MAX_VALUE;
			maxEndTaskId = -1;
			
		}
		
		// check for each executor how many thread add/remove
		List<ExecutorDetails> executorDetailsList;
		for (Component c : components) {
			executorDetailsList = executorToComponentMap.get(c);
			int oldParallelism = executorDetailsList.size();
			int increase = c.getParallelism() - oldParallelism;
			
			if (increase != 0) {
				logger.info("Operator " + c.getName() + " needs to be scaled.");
				logger.info("\t- current parallelism: " + oldParallelism);
				logger.info("\t- new parallelism: " + c.getParallelism());
				logger.info("\t- increase: " + increase);
			}
				
			// get max endTaskId and min startTaskId
			int taskId = componentMinTask.get(c);
			maxEndTaskId = componentMaxTask.get(c);
			minStartTaskId = componentMinTask.get(c);
			
			// compute the new executor numbers and task per executor
			int newExecutorNumber = executorToComponentMap.get(c).size() + increase;
			int newTaskPerExecutor = (int) Math.floor((1 + maxEndTaskId - minStartTaskId) / newExecutorNumber);
			int rest = (1 + maxEndTaskId - minStartTaskId) % newExecutorNumber;
			
			if (increase != 0) {
				if (newExecutorNumber != c.getParallelism())
					logger.warn("Something goes wrong! Parallelism is " + c.getParallelism() + ", but new executor number should be " + newExecutorNumber);
				logger.info("\t - new executor number: " + newExecutorNumber);
				logger.info("\t - new task per executor: " + newTaskPerExecutor); 
			}
				
			// now, create newExecutorNumber new executors and set the taskId
			ExecutorDetails newEd;
			for (int i = 0; i< newExecutorNumber; ++i) {
				
				/* metodo 1 non funzionava con 3 executor e 4 task
				int executorNumber = (1 + maxEndTaskId - minStartTaskId);
				if ( (executorNumber != 1) && (executorNumber % 2 != 0) && (i == 0) ) {
					newEd = new ExecutorDetails(taskId, taskId + newTaskPerExecutor);
					newExecutorToComponentMap.put(newEd, c.getName());
					taskId += newTaskPerExecutor + 1;
				}
				else {
					newEd = new ExecutorDetails(taskId, taskId + newTaskPerExecutor - 1);
					newExecutorToComponentMap.put(newEd, c.getName());
					taskId += newTaskPerExecutor;
				}
				if (increase != 0) 
					logger.info("\t Associated " + newEd.toString() + " to " + c.getName());
				*/
				
				/* metodo 2 TODO da provare */
				if (i < rest) {
					newEd = new ExecutorDetails(taskId, taskId + newTaskPerExecutor);
					newExecutorToComponentMap.put(newEd, c.getName());
					taskId += newTaskPerExecutor + 1;
				}
				else {
					newEd = new ExecutorDetails(taskId, taskId + newTaskPerExecutor - 1);
					newExecutorToComponentMap.put(newEd, c.getName());
					taskId += newTaskPerExecutor;
				}
				if (increase != 0) 
					logger.info("\t Associated " + newEd.toString() + " to " + c.getName());
				
			}
		}
		return newExecutorToComponentMap;
	}
		
		
	/**
	 * Thread delegated to rebalance the topology
	 * @param topologyName
	 * @param rebalanceOptions
	 */
	private void executeRebalance(final String topologyName, final RebalanceOptions rebalanceOptions) {
		new Thread() {
			
			final String threadId = "[rebalance_thread] ";
			
			@Override
			public void run() {
					@SuppressWarnings("rawtypes")
					Map stormConf = Utils.readStormConfig();
					NimbusClient nimbusClient = null;
			        try {
			        	nimbusClient = NimbusClient.getConfiguredClient(stormConf);
			            Client client = nimbusClient.getClient();
			            logger.info(threadId + "NimbusClient and Client objects created. Invoking rebalance..");
			            client.rebalance(topologyName, rebalanceOptions);
			            logger.info(threadId + "Rebalance launched succesfully.");
			        } catch (Exception e) {
			        	logger.error(threadId + "Error while calling rebalance for topology " +
			        					topologyName, e);
			        } finally {
			            if (nimbusClient != null) {
			            	nimbusClient.close();
			            }
			            else logger.warn(threadId + "Something wrong... nimbusClient is null.");
			        }
			}
		}.start();
	}

}
