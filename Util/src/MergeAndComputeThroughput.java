

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.PrintStream;

public class MergeAndComputeThroughput {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		final String folderString = args[0];
		
		File folder = new File(folderString);
		if (!folder.exists() || !folder.isDirectory())
			throw new Exception(folderString + " doesn't exist or is not a folder");
		File mergeFile = new File(folder, "merge");
		mergeFile.delete();
		
		System.out.println("Listing partial throughput files in " + folder + " ...");
		String files[] = folder.list(new FilenameFilter() {
			@Override
			public boolean accept(File arg0, String arg1) {
				return !arg1.startsWith("merge");
			}
		});
		
		System.out.println("Files found:");
		for (int i = 0; i < files.length; i++)
			System.out.println("- " + files[i]);
		
		System.out.println("Create arrays for BufferedReaders and lastEntries");
		BufferedReader readers[] = new BufferedReader[files.length];
		File files_[] = new File[files.length];
		String lastEntries[] = new String[files.length];
		for (int i = 0; i < files.length; i++){
			files_[i] = new File(folder, files[i]);
			//System.out.println("file "+files[i]+" read succesfully. Path: "+files_[i].getAbsolutePath()+"\nOpening reader..");
			readers[i] = new BufferedReader(new FileReader(files_[i]));
		}
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
		
		System.out.print("Creating throughput file...");
		PrintStream throughputStream = new PrintStream(new File(mergeFile.getParent(), "throughput"));
		BufferedReader mergeReader = new BufferedReader(new FileReader(mergeFile));
		String entry;
		int secondCounter = 0;
		int throughputCounter = 0;
		long beginSecondTS = 0;
		long beginAggregate = System.currentTimeMillis();
		while ((entry = mergeReader.readLine()) != null) {
			long ts = getTs(entry);
			int throughput = getThroughput(entry);
			
			if (beginSecondTS == 0)
				beginSecondTS = ts;

			if (ts - beginSecondTS > 1000) {
				throughputStream.println(secondCounter + "," + throughputCounter);
				int secondPassed = (int)Math.ceil((double)(ts - beginSecondTS - 1000) / 1000);
				for (int i = secondCounter + 1; i < secondCounter + secondPassed; i++)
					throughputStream.println(i + ",0");
				secondCounter += secondPassed;
				beginSecondTS += secondPassed * 1000;
				throughputCounter = 0;
			}
			
			throughputCounter += throughput;
		}
		
		// write latency and throughput for the last second
		/*double secondLatency = (double)latencyCounter / throghputCounter;
		latencyStream.println(secondLatency);
		throughputStream.println(throghputCounter);*/
		
		// close i/o streams
		mergeReader.close();
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

	public static int getThroughput(String entry) {
		int latency = -1;
		try {
			latency = Integer.parseInt(entry.substring(entry.indexOf(',') + 1));
		} catch (NumberFormatException e) {
			System.err.println("Error getting throughput from entry: " + entry);
			throw new RuntimeException(e);
		}
		return latency;
	}
}
