package midlab.storm.autoscaling.forecasting;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;

import midlab.storm.autoscaling.topology.Topology;

public class TopologyDeserializer {

	private Topology t;
	
	public void deserialzeTopology(String topologyFileName) throws Exception{
		FileInputStream fin = new FileInputStream(topologyFileName);
		ObjectInputStream ois = new ObjectInputStream(fin);
		t = (Topology) ois.readObject();
		ois.close(); 
	}
	
	public Topology getTopology(){
		return this.t;
	}
	
}
