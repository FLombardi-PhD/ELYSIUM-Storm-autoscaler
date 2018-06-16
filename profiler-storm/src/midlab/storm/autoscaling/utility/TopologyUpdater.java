package midlab.storm.autoscaling.utility;

import java.io.File;
import java.util.ArrayList;
import java.util.Set;

import org.encog.neural.networks.BasicNetwork;
import org.encog.persist.EncogDirectoryPersistence;

import midlab.storm.autoscaling.forecasting.TopologyDeserializer;
import midlab.storm.autoscaling.profiler.Profiler;
import midlab.storm.autoscaling.topology.Component;
import midlab.storm.autoscaling.topology.Topology;

public class TopologyUpdater {

	public static void main(String[] args) throws Exception{
		
		TopologyDeserializer deserializer = new TopologyDeserializer();
		deserializer.deserialzeTopology(new File("resources/topology_ser","topology.ser").getAbsolutePath());
		Topology t = deserializer.getTopology();	
		
		
		//searching for all components in topology and storing their names
		Set<Component> components = t.getLoadTable().keySet();
		String componentName = "";
		ArrayList<String> componentsName = new ArrayList<String>();
		for(Component c : components){
			componentName = c.getName();
			System.out.println("founded "+componentName);
			componentsName.add(componentName);
		}
		
		//for each component (having its name) updating its neural network
		for(String name : componentsName){
			System.out.println("calling method to update network of "+name+"..");
			t.updateNeuralNetworkInComponent(name, (BasicNetwork)EncogDirectoryPersistence.loadObject(new File("resources/neural_networks","net_"+name+".eg")));
		}
		
		System.out.println("storing new topology..");
		Profiler.serializeTopology(t);
	}
	
}
