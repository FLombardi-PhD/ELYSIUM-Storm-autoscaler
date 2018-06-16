package midlab.storm.autoscaling.utility;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;

import org.apache.log4j.Logger;
import org.jgrapht.graph.DefaultWeightedEdge;
import org.jgrapht.graph.ListenableDirectedWeightedGraph;

import midlab.storm.autoscaling.database.DatabaseConnection;
import midlab.storm.autoscaling.topology.Component;

/**
 * Graph utility
 * @author Federico Lombardi - Sapienza University of Rome
 *
 */
public class GraphUtils {

	@SuppressWarnings("unused")
	private static final Logger logger = Logger.getLogger(GraphUtils.class);

	
	/**
	 * Given a graph and a component return the total load by aggregating the load of each task related to the component
	 * @param g the input graph
	 * @param c the component
	 * @return the total load of component c
	 * @throws Exception 
	 */
	public static long getTotalLoad(ListenableDirectedWeightedGraph<Component,DefaultWeightedEdge> g, Component c) throws Exception{
		ArrayList<Integer> tasks = c.getTasks();
		Iterator<Integer> itTask = tasks.iterator();
		String task = "'"+itTask.next().toString()+"'";
		long totLoad = 0;
		while(itTask.hasNext()){
			task += " OR TASK_ID='"+itTask.next().toString()+"'";
		}
		
		String query = "select LOAD from APP.LOAD where TASK_ID="+task;
		//System.out.println(query);
		query = query.replaceAll("'", "");
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		
		conn = DatabaseConnection.connect();
		ps = conn.prepareStatement(query);
		rs = ps.executeQuery();
		while (rs.next()){
			totLoad += Long.parseLong(rs.getString(1));
			//System.out.println("adding load "+rs.getString(1));
		}
		
		if (conn!=null)
			conn.close();
		if (ps!=null)
			ps.close();
		if (rs!=null)
			rs.close();
		
		return totLoad;
	}
	
	/**
	 * Get the total stream (traffic) in tuple/sec read from db flowing into the given task
	 * @param task
	 * @return
	 * @throws Exception 
	 */
	public static long getStreamInTask(int task) throws Exception{
		String query = "select sum(TRAFFIC) from APP.TRAFFIC where DESTINATION_TASK_ID="+task;
		long streamIn = 0;
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		
		conn = DatabaseConnection.connect();
		ps = conn.prepareStatement(query);
		rs = ps.executeQuery();
		
		if(rs.next()) streamIn = rs.getLong(1);
		
		if (conn!=null)
			conn.close();
		if (ps!=null)
			ps.close();
		if (rs!=null)
			rs.close();
		
		return streamIn;
	}
	
	/**
	 * Get the total stream (traffic) in tuple/sec read from db flowing out of the given task
	 * @param task
	 * @return
	 * @throws Exception 
	 */
	public static long getStreamOutTask(int task) throws Exception{
		String query = "select sum(TRAFFIC) from APP.TRAFFIC where SOURCE_TASK_ID="+task;
		long streamOut = 0;
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		
		conn = DatabaseConnection.connect();
		ps = conn.prepareStatement(query);
		rs = ps.executeQuery();
		
		if(rs.next()) streamOut = rs.getLong(1);
		
		if (conn!=null)
			conn.close();
		if (ps!=null)
			ps.close();
		if (rs!=null)
			rs.close();
		
		return streamOut;
	}
	
	/**
	 * Get the cpu load of an executor with the given taskId
	 * @param task
	 * @return
	 * @throws Exception
	 */
	public static long getLoadOfTask(int task) throws Exception{
		String query = "select LOAD from APP.LOAD where TASK_ID="+task;
		long taskLoad = 0;
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		
		conn = DatabaseConnection.connect();
		ps = conn.prepareStatement(query);
		rs = ps.executeQuery();
		
		if(rs.next()) taskLoad = rs.getLong(1);
		
		if (conn!=null)
			conn.close();
		if (ps!=null)
			ps.close();
		if (rs!=null)
			rs.close();
		
		return taskLoad;
	}
	
	/**
	 * Given a component, return the total input stream of this component
	 * @param g the input graph
	 * @param c the component
	 * @return a long representing the total input stream of component c
	 */
	public static long getTotalStreamIn(ListenableDirectedWeightedGraph<Component,DefaultWeightedEdge> g, Component c){
		long totInStream = 0;
		Set<DefaultWeightedEdge> edgesIn = g.incomingEdgesOf(c);
		Iterator<DefaultWeightedEdge> itEdges = edgesIn.iterator();
		DefaultWeightedEdge edge;
		while(itEdges.hasNext()){
			edge = itEdges.next();
			totInStream += g.getEdgeWeight(edge);
		}
		return totInStream;
	}
	
	/**
	 * Given a component, returns the total output stream of this component
	 * @param g the input graph
	 * @param c the component
	 * @return a long representing the total output stream of component c
	 */
	public static long getTotalStreamOut(ListenableDirectedWeightedGraph<Component,DefaultWeightedEdge> g, Component c){
		long totOutStream = 0;
		Set<DefaultWeightedEdge> edgesOut = g.outgoingEdgesOf(c);
		Iterator<DefaultWeightedEdge> itEdges = edgesOut.iterator();
		DefaultWeightedEdge edge;
		while(itEdges.hasNext()){
			edge = itEdges.next();
			totOutStream += g.getEdgeWeight(edge);
		}
		return totOutStream;
	}
	
	/**
	 * Get the task list of a worker node
	 * @param workerHostname
	 * @return
	 * @throws Exception
	 */
	public static ArrayList<Integer> getTaskListInWorker(String workerHostname) throws Exception{
		String query = "select TASK_ID from APP.LOAD where WORKER_HOSTNAME='"+workerHostname+"'";
		ArrayList<Integer> taskList = new ArrayList<Integer>();
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		conn = DatabaseConnection.connect();
		ps = conn.prepareStatement(query);
		rs = ps.executeQuery();
		while(rs.next())
			taskList.add(rs.getInt(1));
		
		if (conn!=null)
			conn.close();
		if (ps!=null)
			ps.close();
		if (rs!=null)
			rs.close();
		return taskList;
	}
	
	/**
	 * Get the total summed input traffic of the executor in a worker node
	 * @param workerHostname
	 * @return
	 * @throws Exception
	 */
	public static int getSummedExecutorInputTrafficInWorker(String workerHostname) throws Exception{
		ArrayList<Integer> taskList = getTaskListInWorker(workerHostname);
		String query = "select sum(TRAFFIC) from APP.TRAFFIC where DESTINATION_TASK_ID=";
		int result = 0;
		
		Iterator<Integer> itTask = taskList.iterator();
		query += ""+itTask.next().toString()+"";
		while(itTask.hasNext()){
			query += " OR DESTINATION_TASK_ID="+itTask.next().toString()+"";	
		}
		
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		conn = DatabaseConnection.connect();
		ps = conn.prepareStatement(query);
		rs = ps.executeQuery();
		if(rs.next())
			result = rs.getInt(1);
		
		if (conn!=null)
			conn.close();
		if (ps!=null)
			ps.close();
		if (rs!=null)
			rs.close();
		
		return result;
	}
	
	/**
	 * Get the total summed output traffic of the executor in a worker node
	 * @param workerHostname
	 * @return
	 * @throws Exception
	 */
	public static int getSummedExecutorOutputTrafficInWorker(String workerHostname) throws Exception{
		ArrayList<Integer> taskList = getTaskListInWorker(workerHostname);
		String query = "select sum(TRAFFIC) from APP.TRAFFIC where SOURCE_TASK_ID=";
		int result = 0;
		
		Iterator<Integer> itTask = taskList.iterator();
		query += ""+itTask.next().toString()+"";
		while(itTask.hasNext()){
			query += " OR SOURCE_TASK_ID="+itTask.next().toString()+"";	
		}
		
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		conn = DatabaseConnection.connect();
		ps = conn.prepareStatement(query);
		rs = ps.executeQuery();
		if(rs.next())
			result = rs.getInt(1);
		
		if (conn!=null)
			conn.close();
		if (ps!=null)
			ps.close();
		if (rs!=null)
			rs.close();
		
		return result;
	}

	/**
	 * Get the total summed cpu load of the executor in a worker node
	 * @param workerHostname
	 * @return
	 * @throws Exception
	 */
	public static long getSummedExecutorLoadInWorker(String workerHostname) throws Exception{
		String query = "select sum(LOAD) from APP.LOAD where WORKER_HOSTNAME='"+workerHostname+"'";
		long result = 0L;
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		conn = DatabaseConnection.connect();
		ps = conn.prepareStatement(query);
		rs = ps.executeQuery();
		if(rs.next())
			result = rs.getLong(1);
		
		if (conn!=null)
			conn.close();
		if (ps!=null)
			ps.close();
		if (rs!=null)
			rs.close();
		
		return result;
	}
	
	/**
	 * Get the worker node cpu usage, read from the db from the APP.WORKER table that reads these values from /proc/stat
	 * @param workerHostname
	 * @return
	 * @throws Exception
	 */
	public static int getWorkerCpuUsage(String workerHostname) throws Exception{
		String query = "select CPU_USAGE from APP.WORKER where HOSTNAME='"+workerHostname+"'";
		int result = 0;
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		conn = DatabaseConnection.connect();
		ps = conn.prepareStatement(query);
		rs = ps.executeQuery();
		if(rs.next())
			result = rs.getInt(1);
		
		if (conn!=null)
			conn.close();
		if (ps!=null)
			ps.close();
		if (rs!=null)
			rs.close();
		
		return result;
	}
	
	/**
	 * Returns the tuple size between two node of an edge
	 * @param g the input graph
	 * @param c1 the first component
	 * @param c2 the second component
	 * @return the aggregation per component of the avg tuple size
	 * @throws Exception 
	 */
	public static double getTupleSize(ListenableDirectedWeightedGraph<Component,DefaultWeightedEdge> g, Component c1, Component c2) throws Exception{
		return aggregateTupleSize(g, c1, c2);
	}
	
	/**
	 * Given a graph, update the weight of each edge by calling the method aggragateTraffic
	 * @param g the input grapg
	 * @return the updated graph
	 * @throws Exception 
	 */
	public static ListenableDirectedWeightedGraph<Component,DefaultWeightedEdge> updateWeight(ListenableDirectedWeightedGraph<Component,DefaultWeightedEdge> g) throws Exception{
		Set<DefaultWeightedEdge> edgeSet = g.edgeSet();
		Iterator<DefaultWeightedEdge> itEdge = edgeSet.iterator();
		Component c1;
		Component c2;
		DefaultWeightedEdge e;
		while(itEdge.hasNext()){
			e = itEdge.next();
			c1 = g.getEdgeSource(e);
			c2 = g.getEdgeTarget(e);
			g = aggregateTraffic(g, c1, c2);
		}
		return g;
	}
	
	/**
	 * Given a graph, returns the current date taken from the database
	 * @param g the input graph
	 * @return a String containing the date
	 * @throws Exception 
	 */
	public static String getCurrentDate(ListenableDirectedWeightedGraph<Component,DefaultWeightedEdge> g) throws Exception{
		String query = "select TS from APP.TRAFFIC";
		String date = "";
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		conn = DatabaseConnection.connect();
		ps = conn.prepareStatement(query);
		rs = ps.executeQuery();
		while(rs.next()){
			//System.out.println("date read: "+rs.getString(1));
			if(rs.getString(1)!=null && rs.getString(1)!=""){
				date = rs.getString(1);
				break;
			}
		}
		
		if (conn!=null)
			conn.close();
		if (ps!=null)
			ps.close();
		if (rs!=null)
			rs.close();
		
		return date;
	}
	
	/**
	 * Given two vertexes, aggregates the total traffic between tasks related to the given vertexes
	 * @param g the input graph
	 * @param c1 the first component (vertex)
	 * @param c2 the second component (vertex)
	 * @return the updated graph
	 * @throws Exception 
	 */
	public static ListenableDirectedWeightedGraph<Component,DefaultWeightedEdge> aggregateTraffic(ListenableDirectedWeightedGraph<Component,DefaultWeightedEdge> g, Component c1, Component c2) throws Exception{
		int totWeight = 0;
		
		ArrayList<Integer> sourceTask = c1.getTasks();
		ArrayList<Integer> destTask = c2.getTasks();
		
		Iterator<Integer> itSource = sourceTask.iterator();
		String source = "'"+itSource.next().toString()+"'";
		while(itSource.hasNext()){
			source += " OR SOURCE_TASK_ID='"+itSource.next().toString()+"'";	
		}
		
		Iterator<Integer> itDest = destTask.iterator();
		String dest = "'"+itDest.next().toString()+"'";
		while(itDest.hasNext()){
			dest += " OR DESTINATION_TASK_ID='"+itDest.next().toString()+"'";	
		}
		
		String query = "select TRAFFIC from APP.TRAFFIC where (SOURCE_TASK_ID="+source+") AND (DESTINATION_TASK_ID="+dest+")";
		query = query.replaceAll("'", "");
		//System.out.println(query);
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		conn = DatabaseConnection.connect();
		ps = conn.prepareStatement(query);
		rs = ps.executeQuery();
		while(rs.next()){
			totWeight += Integer.parseInt(rs.getString(1));
			//System.out.println("adding weight "+rs.getString(1));
		}
		//System.out.println("tot weight = "+totWeight);
		
		if (conn!=null)
			conn.close();
		if (ps!=null)
			ps.close();
		if (rs!=null)
			rs.close();
		
		//setting the weight to the edge
		g.setEdgeWeight(g.getEdge(c1, c2), totWeight);
		//System.out.println("\n***edge: "+c1.getName()+"-"+c2.getName()+"; new weight to insert="+totWeight+".. done. Current weight= "+g.getEdgeWeight(g.getEdge(c1, c2)));
		return g;
	}
	
	/**
	 * Aggregates the tuple size per component and calculates the average value
	 * @param g the input graph
	 * @param c1 the first component as the source node of the edge
	 * @param c2 the second component as the destination node of the edge
	 * @return a double containing the avg tuple size value
	 * @throws Exception 
	 */
	public static double aggregateTupleSize(ListenableDirectedWeightedGraph<Component,DefaultWeightedEdge> g, Component c1, Component c2) throws Exception{
		long tupleSizeSum = 0;
		long tupleSizeCount = 0;
		double tupleSizeAvg = 0.0;
		
		ArrayList<Integer> sourceTask = c1.getTasks();
		ArrayList<Integer> destTask = c2.getTasks();
		
		Iterator<Integer> itSource = sourceTask.iterator();
		String source = "'"+itSource.next().toString()+"'";
		while(itSource.hasNext()){
			source += " OR SOURCE_TASK_ID='"+itSource.next().toString()+"'";	
		}
		
		Iterator<Integer> itDest = destTask.iterator();
		String dest = "'"+itDest.next().toString()+"'";
		while(itDest.hasNext()){
			dest += " OR DESTINATION_TASK_ID='"+itDest.next().toString()+"'";	
		}
		
		String query = "select TUPLESIZESUM,TUPLESIZECOUNT from APP.TUPLESIZE where (SOURCE_TASK_ID="+source+") AND (DESTINATION_TASK_ID="+dest+")";
		query = query.replaceAll("'", "");
		//System.out.println(query);
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		conn = DatabaseConnection.connect();
		ps = conn.prepareStatement(query);
		rs = ps.executeQuery();
		while(rs.next()){
			tupleSizeSum += Integer.parseInt(rs.getString(1));
			tupleSizeCount += Integer.parseInt(rs.getString(2));
			//System.out.println("adding weight "+rs.getString(1));
		}
		//System.out.println("tot weight = "+totWeight);
		
		if (conn!=null)
			conn.close();
		if (ps!=null)
			ps.close();
		if (rs!=null)
			rs.close();
		
		tupleSizeAvg = tupleSizeSum/tupleSizeCount;
		return tupleSizeAvg;
	}
	
	/**
	 * Print the graph with vertexes, edges and related weights
	 * @param g the input graph
	 * @throws Exception 
	 */
	public static void printGraph(ListenableDirectedWeightedGraph<Component,DefaultWeightedEdge> g) throws Exception{
		System.out.println("\n*** UPDATING GRAPH ***");
		Set<Component> components = g.vertexSet();
		Iterator<Component> itComponent = components.iterator();
		Component currentComponent;
		long load = 0;
		while(itComponent.hasNext()){
			currentComponent = itComponent.next();
			load = getTotalLoad(g, currentComponent);
			System.out.println("component: "+currentComponent.toString()+" load = "+load);
		}
		
		Set<DefaultWeightedEdge> edges = g.edgeSet();
		Iterator<DefaultWeightedEdge> itEdge = edges.iterator();
		DefaultWeightedEdge edge;
		while(itEdge.hasNext()){
			edge = itEdge.next();
			System.out.println("edge "+g.getEdgeSource(edge)+"-"+g.getEdgeTarget(edge)+" w="+g.getEdgeWeight(edge));
		}
	}
}
