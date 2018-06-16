package midlab.storm.profiler;

import java.util.HashMap;
import java.util.Map;

import midlab.storm.scheduler.db.CommonDataManager;

import org.apache.log4j.Logger;

import backtype.storm.hooks.BaseTaskHook;
import backtype.storm.hooks.info.BoltExecuteInfo;
import backtype.storm.serialization.KryoValuesSerializer;
import backtype.storm.task.TopologyContext;

public class TupleSizeProfilerHook extends BaseTaskHook {
	
	private Logger logger = Logger.getLogger(TupleSizeProfilerHook.class);
	
	private KryoValuesSerializer serializer;
	private String topologyId;
	private String componentId;
	private int taskId;
	
	private long lastTS;
	
	/**
	 * map streamId -> stats on tuple size
	 */
	private Map<String, SumAndCount> sizeMap = new HashMap<String, TupleSizeProfilerHook.SumAndCount>();

	class SumAndCount {
		int sum;
		long count;
		
		void incr(long size) {
			sum += size;
			count++;
		}
		
		long avg() {
			return sum / count;
		}
	}
	
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context) {
		serializer = new KryoValuesSerializer(conf);
		topologyId = context.getStormId();
		componentId = context.getThisComponentId();
		taskId = context.getThisTaskId();
	}
	
	@Override
	public void boltExecute(BoltExecuteInfo info) {
		String streamId = info.tuple.getSourceStreamId();
		SumAndCount sac = sizeMap.get(streamId);
		if (sac == null) {
			sac = new SumAndCount();
			sizeMap.put(streamId, sac);
		}
		int size = serializer.serializeObject(info.tuple.getValues()).length;
		sac.incr(size);
		// logger.debug("stream " + streamId + ": received tuple with dimension " + size + " bytes (" + info.tuple.getValues().toString() + ")");
		
		if (lastTS == 0)
			lastTS = System.currentTimeMillis();
		long now = System.currentTimeMillis();
		if (now-lastTS >= 60000) {
			lastTS = now;
			logger.info("=================================");
			logger.info("Tuple size stats for component: " + componentId + " (task ID: " + taskId + ")");
			for (String stream : sizeMap.keySet()) {
				sac = sizeMap.get(stream);
				logger.info("input stream " + stream + "(from task " + info.tuple.getSourceTask() + " to task " + taskId + "): " + sac.avg() + " bytes (sum: " + sac.sum + ", count: " + sac.count + ")");
			}
			logger.info("=================================");
			
			try {
				CommonDataManager.getInstance().updateTupleSize(topologyId, info.tuple.getSourceTask(), taskId, sac.sum, sac.count);
			} catch (Exception e) {
				logger.error("Error updating tuple size table", e);
			}
		}
	}
}
