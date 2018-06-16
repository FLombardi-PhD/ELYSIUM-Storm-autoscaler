

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.PrintStream;

public class LatencyThroughputCalculator {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		final String topologyID = args[0];
		
		File mergeFile = new File("latencies/" + topologyID + "/" + topologyID + ".merge.latency");
		mergeFile.delete();
		
		System.out.println("Listing files for topology " + topologyID + " ...");
		File dir = new File("latencies/"+topologyID);
		String files[] = dir.list(new FilenameFilter() {
			@Override
			public boolean accept(File arg0, String arg1) {
				return arg1.startsWith(topologyID);
			}
		});
		
		System.out.println("Files found:");
		for (int i = 0; i < files.length; i++)
			System.out.println("- " + files[i]);
		
		System.out.println("Create arrays for BufferedReaders and lastEntries");
		BufferedReader readers[] = new BufferedReader[files.length];
		String lastEntries[] = new String[files.length];
		for (int i = 0; i < files.length; i++)
			readers[i] = new BufferedReader(new FileReader("latencies/" + topologyID + "/" + files[i]));
		
		System.out.print("Merge-sorting all the entries to " + mergeFile.getAbsolutePath() + " ...");
		PrintStream mergeStream = new PrintStream(mergeFile);
		long beginMergeTS = System.currentTimeMillis();
		while(true) {
			
			// prepare one entry for each file
			for (int i = 0; i < files.length; i++)
				if (lastEntries[i] == null)
					lastEntries[i] = readers[i].readLine();
			
			// check whether to break
			boolean b = true;
			for (int i = 0; i < files.length; i++)
				if (lastEntries[i] != null) {
					b = false;
					break;
				}
			if (b) break;
			
			// identify the oldest timestamp
			int oldestIndex = -1;
			long oldestTS = -1;
			for (int i = 0; i < files.length; i++) {
				if (lastEntries[i] != null && (oldestTS == -1 || getTs(lastEntries[i]) < oldestTS)) {
					oldestIndex = i;
					oldestTS = getTs(lastEntries[i]);
				}
			}
			
			// print it to merged stream
			mergeStream.println(lastEntries[oldestIndex]);
			
			// nullify related last entry to force the load of next entry for that file
			lastEntries[oldestIndex] = null;
		}
		
		// close i/o streams
		mergeStream.close();
		for (int i = 0; i < files.length; i++)
			readers[i].close();
		
		System.out.println("Merge-sort completed in " + (System.currentTimeMillis() - beginMergeTS) + " ms");
		
		System.out.print("Creating latency and throughput files...");
		PrintStream latencyStream = new PrintStream(new File(mergeFile.getParent(), "latency"));
		PrintStream throughputStream = new PrintStream(new File(mergeFile.getParent(), "throughput"));
		BufferedReader mergeReader = new BufferedReader(new FileReader(mergeFile));
		String entry;
		int latencyCounter = 0;
		int entryCount = 0;
		long beginSecondTS = 0;
		int secondCounter = 0;
		long beginAggregate = System.currentTimeMillis();
		while ((entry = mergeReader.readLine()) != null) {
			long ts = getTs(entry);
			int latency = getLatency(entry);
			
			if (beginSecondTS == 0)
				beginSecondTS = ts;

			if (ts - beginSecondTS > 1000) {
				int secondLatency = latencyCounter / entryCount;
				latencyStream.println(secondCounter + "," + secondLatency);
				throughputStream.println(secondCounter + "," + entryCount);
				
				int elapsedSeconds = (int)Math.ceil((double)(ts - beginSecondTS - 1000) / 1000);
				for (int i = secondCounter + 1; i < secondCounter + elapsedSeconds; i++) {
					throughputStream.println();
					latencyStream.println();
				}
				secondCounter += elapsedSeconds;
				beginSecondTS += elapsedSeconds * 1000;
				latencyCounter = 0;
				entryCount = 0;
			}
			
			latencyCounter += latency;
			entryCount++;
		}
		
		// write latency and throughput for the last second
		int secondLatency = latencyCounter / entryCount;
		latencyStream.println(secondCounter + "," + secondLatency);
		throughputStream.println(secondCounter + "," + entryCount);
		
		// close i/o streams
		mergeReader.close();
		latencyStream.close();
		throughputStream.close();
		
		System.out.println("completed in " + (System.currentTimeMillis() - beginAggregate) + " ms");
	}
	
	public static long getTs(String entry) {
		long ts = -1;
		try {
			ts = Long.parseLong(entry.substring(0, entry.indexOf(',')));
		} catch (NumberFormatException e) {
			System.err.println("Error getting timestamp from entry: " + entry);
			throw new RuntimeException(e);
		}
		return ts;
	}

	public static int getLatency(String entry) {
		int latency = -1;
		try {
			// latency is in nanoseconds, convert it in milliseconds
			latency = (int)(Long.parseLong(entry.substring(entry.indexOf(',') + 1)) / 1000000);
		} catch (NumberFormatException e) {
			System.err.println("Error getting latency from entry: " + entry);
			throw new RuntimeException(e);
		}
		return latency;
	}
}
