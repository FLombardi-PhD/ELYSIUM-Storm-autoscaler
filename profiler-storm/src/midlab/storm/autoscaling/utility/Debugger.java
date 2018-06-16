package midlab.storm.autoscaling.utility;

import java.io.File;
import java.util.Date;
import java.util.Set;

import org.encog.ml.data.MLData;
import org.encog.ml.data.basic.BasicMLData;
import org.encog.neural.networks.BasicNetwork;

import midlab.storm.autoscaling.forecasting.TopologyDeserializer;
import midlab.storm.autoscaling.topology.Component;
import midlab.storm.autoscaling.topology.Topology;

public class Debugger {

	public static void main(String[] args) throws Exception{
		TopologyDeserializer deserializer = new TopologyDeserializer();
		deserializer.deserialzeTopology(new File("resources/topology_ser","topology.ser").getAbsolutePath());
		Topology t = deserializer.getTopology();
		System.out.println("max traffic/min: "+t.getMaxTrafficPerMinute()+"; max traffic/sec:"+t.getMaxTrafficPerSecond());
		
		/** check time stamp
		long timestamp = 1398815400;
		Date date = new Date(timestamp*1000);
		
		String res = Utils.convertTimestampInNormalizedDate(timestamp);
		
		System.out.println(date+"\n"+res);
		*/
		
		String componentName = "wordGenerator";
		double[] input = {0.0580068143100511075};
		Set<Component> components = t.getGraph().vertexSet();
		for( Component c : components){
			if(c.getName().equals(componentName)){
				BasicNetwork net = t.getLoadTable().get(c);
				BasicMLData data = new BasicMLData(input);
				MLData computation = net.compute(data);
				double[] output = computation.getData();
				System.out.println(output[0]);
			}
			 
		}
	}
}
