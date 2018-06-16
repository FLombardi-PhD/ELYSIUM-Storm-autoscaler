

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.LineNumberReader;
import java.io.PrintWriter;

public class PlotDataGenerator {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
		
		String folderString = "";
		File folder = null;
		int k = -1;
		
		boolean emittedThrougput = false;
		String folderEmittedThroughputString = "";
		File folderEmittedThroughput = null;
		
		//checking the corectness of the first parameter args[0]
		try{
			folderString = args[0];
			folder = new File(folderString);
			if (!folder.exists() || !folder.isDirectory())
				throw new Exception(folderString + " doesn't exist or is not a folder");
		}catch(Exception e){
			throw new Exception("The first parameter has to be the latency path");
		}
		
		//checking the correctness of the second parameter args[1]
		try{
			k = Integer.parseInt(args[1]);
		}catch(Exception e){
			throw new Exception("The second parameter has to be an intger as the scale factor");
		}
		
		//checking the correctness of the third parameter args[2]
		try{
			folderEmittedThroughputString = args[2];
			folderEmittedThroughput = new File(folderEmittedThroughputString);
			if (!folderEmittedThroughput.exists() || !folderEmittedThroughput.isDirectory())
				throw new Exception(folderEmittedThroughputString + " doesn't exist or is not a folder");
			emittedThrougput = true;
			System.out.println("Computing plot data using emitted troughput data");
		}catch(ArrayIndexOutOfBoundsException aioobe){
			System.out.println("Computing plot data without using emitted troughput data");
		}
		
		
		
		//getting topology id
		String topologyId = folder.getName();
		System.out.println("Generating plot data for "+topologyId+" with k="+k);
				
		//getting twitter-data-driver througput, througput (completed), latency and optionally throuput (emitted)
		File tddt = getTwitterDataDriverThrougput(folder);
		File thr = new File(folder, "throughput");
		File lat = new File(folder, "latency");
		File ethr = null;
		if(emittedThrougput){
			try{
				ethr = new File(folderEmittedThroughput, "throughput");
				System.out.println("Founded files:\n- "+tddt.getName()+"\n- "+thr.getName()+"\n- "+lat.getName()+"\n- "+ethr.getName());
			}catch(Exception e){
				throw new Exception("Some files missing");
			}
		}
		else{
			try{
				System.out.println("Founded files:\n- "+tddt.getName()+"\n- "+thr.getName()+"\n- "+lat.getName());
			}catch(Exception e){
				throw new Exception("Some files missing");
			}
		}
		
		//opening readers
		BufferedReader tddtReader = new BufferedReader(new FileReader(tddt));
		BufferedReader thrReader = new BufferedReader(new FileReader(thr));
		BufferedReader latReader = new BufferedReader(new FileReader(lat));
		BufferedReader ethrReader = null;
		if(emittedThrougput){
			ethrReader = new BufferedReader(new FileReader(ethr));
		}
		
		//opening output file
		File outFile = new File(folder, topologyId+".csv");
		PrintWriter out = new PrintWriter(outFile);
		if(emittedThrougput) out.println("time;data driver;data driver_avg;spout;spout_avg;latency;latency_avg;emit_throughput;emit_throughput_avg");
		else out.println("time;data driver;data driver_avg;spout;spout_avg;latency;latency_avg;");
		
		String lineTddt;
		String lineThr;
		String lineLat;
		String lineEthr;
		
		String time;
		
		double dataDriver;
		double spout;
		double latency;
		double emitThr = 0.0;
		
		double dataDriverAvg;
		double spoutAvg;
		double latencyAvg;
		double emitThrAvg = 0.0;
				
		double[] lastDataDriver = new double[10];
		double[] lastSpout = new double[10];
		double[] lastLatency = new double[10];
		double[] lastEmitThr = new double[10];
		
		LineNumberReader lnr = new LineNumberReader(new FileReader(tddt));
		lnr.skip(Long.MAX_VALUE);
		int lineNumber = lnr.getLineNumber();
		lnr.close();
		
		long beginTS = System.currentTimeMillis();
		int cont = 0;
		while(cont<lineNumber){
			System.out.println("iteration "+cont);
			lineTddt = tddtReader.readLine();
			lineThr = thrReader.readLine();
			lineLat = latReader.readLine();
			lineEthr = "";
			if(emittedThrougput) lineEthr = ethrReader.readLine();
			
			String[] tddtArr = lineTddt.split(",");
			time = tddtArr[0];
			dataDriver = Double.parseDouble(tddtArr[1]);
			
			//case where the line in latencies/throughput file is empty, we write just time, dataDriver, dataDriverAvg
			if(lineThr.isEmpty() || lineThr.startsWith("\n") || lineThr.equalsIgnoreCase("\n") || lineThr.length()<3){
				if(cont<10){
					dataDriverAvg = dataDriver*k;
					spoutAvg = 0.0;
					latencyAvg = 0.0;
					if(emittedThrougput) emitThrAvg = 0.0;
					lastDataDriver[cont] = dataDriver;
					lastSpout[cont] = 0.0;
					lastLatency[cont] = 0.0;
					if(emittedThrougput) lastEmitThr[cont] = 0.0;
				}
				else{
					lastDataDriver = shiftArray(lastDataDriver);
					lastDataDriver[0] = dataDriver*k;
					dataDriverAvg = avg(lastDataDriver);
					
					lastSpout = shiftArray(lastSpout);
					lastSpout[0] = 0.0;
					
					lastLatency = shiftArray(lastLatency);
					lastLatency[0] = 0.0;
					
					if(emittedThrougput){
						lastEmitThr = shiftArray(lastEmitThr);
						lastEmitThr[0] = 0.0;
					}
					
				}
				String toPrint = ""+time+";"+dataDriver+";"+dataDriverAvg;
				toPrint = toPrint.replaceAll("\\.", ",");
				out.println(toPrint);
			}
			//case where the the line in latencies/throughput file is not empty, we write all the data
			else{
				
				String[] thrArr = lineThr.split(",");
				String[] latArr = lineLat.split(",");
				String[] ethrArr = null;
				if(emittedThrougput) ethrArr = lineEthr.split(",");
				
				spout = Double.parseDouble(thrArr[1]);
				latency = Double.parseDouble(latArr[1]);
				if(emittedThrougput) emitThr = Double.parseDouble(ethrArr[1]);
				
				if(cont<10){
					dataDriverAvg = dataDriver*k;
					spoutAvg = spout;
					latencyAvg = latency;
					if(emittedThrougput) emitThrAvg = emitThr;
					lastDataDriver[cont] = dataDriver;
					lastSpout[cont] = spout;
					lastLatency[cont] = latency;
					if(emittedThrougput) lastEmitThr[cont] = emitThr;
				}
				else{
					lastDataDriver = shiftArray(lastDataDriver);
					lastDataDriver[0] = dataDriver*k;
					dataDriverAvg = avg(lastDataDriver);
					
					lastSpout = shiftArray(lastSpout);
					lastSpout[0] = spout;
					spoutAvg = avg(lastSpout);
					
					lastLatency = shiftArray(lastLatency);
					lastLatency[0] = latency;
					latencyAvg = avg(lastLatency);
					
					if(emittedThrougput){
						lastEmitThr = shiftArray(lastEmitThr);
						lastEmitThr[0] = emitThr;
						emitThrAvg = avg(lastEmitThr);
					}
					
				}
				String toPrint = ""+time+";"+dataDriver+";"+dataDriverAvg+";"+spout+";"+spoutAvg+";"+latency+";"+latencyAvg;
				if(emittedThrougput) toPrint += ";"+emitThr+";"+emitThrAvg;
				toPrint = toPrint.replaceAll("\\.", ",");
				out.println(toPrint);
			}
			out.flush();
			++cont;
			
		}
		System.out.println(cont+" iteration done");
		System.out.println("Plot data in file "+outFile.getPath()+" generated in " + (System.currentTimeMillis() - beginTS) + " ms");
		
		out.close();
		tddtReader.close();
		thrReader.close();
		latReader.close();
		if(emittedThrougput) ethrReader.close();
		
	}
	
	
	
	private static double[] shiftArray(double[] lastArray){
		double[] newArr = new double[lastArray.length];
		for(int i=1; i<lastArray.length; ++i){
			newArr[i] = lastArray[i-1];
		}
		return newArr;
	}
	
	private static double avg(double[] array){
		double sum = 0.0;
		for(int i=0; i<array.length; ++i){
			sum += array[i];
		}
		return sum/(double)array.length;
	}
	
	public static File getTwitterDataDriverThrougput(File folder) {
		for (File fileEntry : folder.listFiles()) {
	        if(fileEntry.getName().startsWith("twitter-data-driver-throughput-")) return fileEntry;
	    }
	    return null;
	}
}

