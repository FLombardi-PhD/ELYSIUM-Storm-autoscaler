package midlab.storm.autoscaling.topology;

import java.io.Serializable;
import java.util.ArrayList;

import org.encog.engine.network.activation.ActivationTANH;
import org.encog.neural.networks.BasicNetwork;
import org.encog.neural.networks.layers.BasicLayer;

/**
 * Storm Component
 * @author Federico Lombardi - Sapienza University of Rome
 *
 */
public abstract class Component implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 4557210410364455295L;
	private String name;
	private ArrayList<Integer> tasks;
	private BasicNetwork network;
	private int parallelism;
	
	/**
	 * Builds a component
	 * @param name the name
	 * @param parallelism its parallelism
	 */
	public Component(String name, int parallelism){
		this.name = name;
		tasks = new ArrayList<Integer>();
		
		//initializing the neural network
		network = new BasicNetwork();
		network.addLayer(new BasicLayer(new ActivationTANH(), true, 1));
		network.addLayer(new BasicLayer(new ActivationTANH(), true, 3));
		network.addLayer(new BasicLayer(new ActivationTANH(), true, 1));
		network.getStructure().finalizeStructure();
		network.reset();
		
		//setting parallelism
		this.parallelism = parallelism;
	}
	
	
	/**
	 * Returns the name of the component
	 * @return a String containing the component name
	 */
	public String getName(){
		return this.name;
	}
	
	/**
	 * Returns a list of tasks
	 * @return an ArrayList of tasks
	 */
	public ArrayList<Integer> getTasks(){
		return this.tasks;
	}
	
	/**
	 * Returns the neural network of the component
	 * @return a BasciNetwork
	 */
	public BasicNetwork getNeuralNetwork(){
		return this.network;
	}
	
	/**
	 * Returns the parallelism of the component
	 * @return an int representing the parallelism
	 */
	public int getParallelism(){
		return this.parallelism;
	}
	
	/**
	 * Adds a task to the list
	 * @param taskId the id of the task to add
	 * @return a boolean equals to 'true' wheter the task has been added, 'false' otherwise
	 */
	public boolean addTask(int taskId){
		return tasks.add(taskId);
	}
	
	public void setParallelism(int parallelism) {
		this.parallelism = parallelism;
	}
	
	/**
	 * Removes a task from the list
	 * @param taskId the id of the task to remove
	 * @return an int indicating the index of the task removed
	 */
	public int removeTask(int taskId){
		return tasks.remove(taskId);
	}
	
	/**
	 * delete all the task in the list
	 */
	public void deleteAllTask(){
		this.tasks = new ArrayList<Integer>();
	}
	
	/**
	 * update the task list
	 * @param newTaskList
	 */
	public void updateTaskList(ArrayList<Integer> newTaskList){
		this.tasks = newTaskList;
	}
	
	/**
	 * Sets a neural network to the component
	 * @param net the new neural network
	 */
	public void setNeuralNetwork(BasicNetwork net){
		this.network = net;
	}
		
	@Override
	public String toString(){
		return this.name;
	}
	
	/**
	 * Return a boolean indicating whether the component is a bolt or not
	 * @return 'true' if the component is a bolt, 'false' otherwise
	 */
	public abstract boolean isBolt();
	
	/**
	 * Return a boolean indicating whether the component is a spout or not
	 * @return 'true' if the component is a spout, 'false' otherwise
	 */
	public abstract boolean isSpout();
	
}
