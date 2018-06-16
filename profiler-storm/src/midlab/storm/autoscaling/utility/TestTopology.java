package midlab.storm.autoscaling.utility;

import java.io.File;
import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;

import org.encog.neural.networks.BasicNetwork;
import org.encog.persist.EncogDirectoryPersistence;
import org.jgrapht.graph.DefaultWeightedEdge;

import midlab.storm.autoscaling.forecasting.TopologyDeserializer;
import midlab.storm.autoscaling.profiler.Profiler;
import midlab.storm.autoscaling.topology.Component;
import midlab.storm.autoscaling.topology.Topology;

public class TestTopology {

	public static void main(String[] args) throws Exception {
		
		TopologyDeserializer deserializer = new TopologyDeserializer();
		deserializer.deserialzeTopology(new File("resources/topology_ser","new_topology.ser").getAbsolutePath());
		Topology t = deserializer.getTopology();
		
		/*
		System.out.println("Current trf=" + t.getTrf() + ". updating new trf and maxTraffic value..");
		t.updateMaxTrafficAndTrf(35220, 20);
		
		System.out.println("deserializing topology..");
		deserializer = new TopologyDeserializer();
		deserializer.deserialzeTopology(new File("resources/topology_ser","new_topology.ser").getAbsolutePath());
		t = deserializer.getTopology();
		*/
		
		System.out.println("maxCpuLoad=" + t.getMaxCpuLoad() + "\n" +
						   "maxTrafficPerMiute=" + t.getMaxTrafficPerMinute() + "\n" +
						   "maxTrafficPerSecond=" + t.getMaxTrafficPerSecond() + "\n" +
						   "trf=" + t.getTrf());
		
		//searching for all components in topology and storing their names
		/*Set<Component> components = t.getLoadTable().keySet();
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
		*/
		String name = "__acker";
		t.updateNeuralNetworkInComponent(name, (BasicNetwork)EncogDirectoryPersistence.loadObject(new File("resources/neural_networks","net_"+name+".eg")));
		System.out.println("storing new topology..");
		serializeTopology(t);
		
	}
	
	/**
	 * Serialize on file the topology
	 * @param t input topology to serialize
	 */
	public static void serializeTopology(Topology t){
		System.out.println("Serializing topology on file..");
		try {
			String path = "resources/topology_ser/"; 
			FileOutputStream fout = new FileOutputStream(path+("new_topology.ser"));
			ObjectOutputStream oos = new ObjectOutputStream(fout);   
			oos.writeObject(t);
			oos.close();
			System.out.println("Topology successfully serialized in "+path+("new_topology.ser"));
		} catch(Exception ex) {
			System.err.println("An error occurs while serializing topology.");
			ex.printStackTrace();
		}
	}
}
