package midlab.storm.autoscaling.forecasting;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.Iterator;
import java.util.Set;

import midlab.storm.autoscaling.topology.Spout;
import midlab.storm.autoscaling.topology.Topology;
import midlab.storm.autoscaling.topology.TopologyBuilder;
import midlab.storm.autoscaling.utility.GraphUtils;

import org.encog.ml.data.MLData;
import org.encog.ml.data.MLDataPair;
import org.encog.ml.data.MLDataSet;
import org.encog.neural.networks.BasicNetwork;
import org.encog.util.csv.CSVFormat;
import org.encog.util.simple.TrainingSetUtil;
import org.encog.persist.EncogDirectoryPersistence;
import org.jgrapht.graph.ListenableDirectedWeightedGraph;

/**
 * Forecasting Engine class
 * @author Federico Lombardi - Sapienza University of Rome
 *
 */
public class TupleRateForecaster {

	public static void validateNeuralNetwork(){
		String dataDir = "D:\\Dropbox\\PhD\\debs2015\\debs-condivisa\\script\\5.encog\\Storm\\";
		final File networkFile = new File(dataDir, "SotrmNN-w=10-h=0.eg");
		BasicNetwork network = null;
		
		try {
			network = (BasicNetwork) EncogDirectoryPersistence.loadObject(networkFile);
		} catch (Exception e) {	e.printStackTrace();
		}
		
		try{
			MLDataSet testSet = TrainingSetUtil.loadCSVTOMemory(CSVFormat.DECIMAL_POINT, dataDir + "test-w=10-h=0.csv", true, 15, 1);
			BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(dataDir+"\\result_data.csv",true)));
			for (MLDataPair pair : testSet){
				final MLData output = network.compute(pair.getInput());
				final MLData ideal = pair.getIdeal();
				
				double predictedValue = output.getData(0);
				double idealValue = ideal.getData(0);
				
				out.write(""+predictedValue+","+idealValue+"\n");
				out.flush();
			}
			testSet.close();
			out.close();
		} catch (Exception e) { e.printStackTrace();
		}
	}
	
	public static long[] getAvgLastMinuteTrafficPerSpout(long[][] lastMinuteTrafficPerSpout){
		long[] avgLastMinuteTrafficPerSpout = new long[lastMinuteTrafficPerSpout.length];
		for(int i=0; i<lastMinuteTrafficPerSpout.length; ++i){
			for(int j=0; j<lastMinuteTrafficPerSpout[0].length; ++j){
				avgLastMinuteTrafficPerSpout[i] += lastMinuteTrafficPerSpout[i][j];
			}
			avgLastMinuteTrafficPerSpout[i] = avgLastMinuteTrafficPerSpout[i] / lastMinuteTrafficPerSpout[0].length;
		}
		return avgLastMinuteTrafficPerSpout;
	}
	
	//TODO: finire
	public static void main(final String args[]) throws Exception{
		
		Topology t = TopologyBuilder.initializeTopology();
		ListenableDirectedWeightedGraph g = t.getGraph();
		Set<Spout> spouts = t.getSpouts();
		Iterator<Spout> itSpouts = spouts.iterator();
		Spout currentSpout;
		long[][] lastMinuteTrafficPerSpout = new long[spouts.size()][6];
		long[] avgLastMinuteTrafficPerSpout = new long[spouts.size()];
		int contSpout = 0;
		
		long currentTime = System.currentTimeMillis();
		
		while(true){
			
			//prendo 6 volte al minuto il traffico per ogni spout (perchè lo prendo ogni 10sec)
			for(int i=0; i<6; ++i){
				//for each spout
				while(itSpouts.hasNext()){
					currentSpout = itSpouts.next();
					//prendo il suo traffico uscente
					lastMinuteTrafficPerSpout[contSpout][i] = GraphUtils.getTotalStreamOut(g, currentSpout);
					++contSpout;
				}
				contSpout = 0;
				Thread.sleep(10000);
			}
			
			//per ogni spout calcolo la media del traffico nel minuto corrente
			avgLastMinuteTrafficPerSpout = getAvgLastMinuteTrafficPerSpout(lastMinuteTrafficPerSpout);
			
			
		}
		
	}
	
	
	
}
