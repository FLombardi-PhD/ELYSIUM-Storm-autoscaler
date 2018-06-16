import java.io.BufferedReader;
import java.io.FileReader;
import java.io.PrintStream;


public class MaxReportsPerSecondCounter {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		
		/*String testGetSecond[] = {
			"0,0,107,23,0,0,1,74,395625,-1,-1,-1,-1,-1,-1", 
			"0,2,136,23,0,0,1,86,458986,-1,-1,-1,-1,-1,-1", 
			"2,104,274,-1,-1,-1,-1,-1,-1,18,-1,-1,-1,-1,-1",
			"0,10784,93477,55,0,4,1,2,11140,-1,-1,-1,-1,-1,-1"};
		for (String line : testGetSecond)
			System.out.println(line + " -> " + getSecond(line));
		System.exit(0);*/
		
		BufferedReader br = new BufferedReader(new FileReader(args[0]));
		PrintStream ps = new PrintStream("out." + System.currentTimeMillis() + ".txt");
		
		int maxSecond = -1;
		int maxReportsPerSecond = -1;
		
		int lastSecond = -1;
		int currentCount = 0;
		
		long t = System.currentTimeMillis();
		long n = 0;
		
		String line = null;
		while ( (line = br.readLine()) != null ) {
			int second = getSecond(line);
			if (second > lastSecond) {
				if (lastSecond > -1) {
					ps.println(lastSecond + "," + currentCount);
					if (currentCount > maxReportsPerSecond) {
						maxReportsPerSecond = currentCount;
						maxSecond = lastSecond;
					}
				}
				currentCount = 0;
				lastSecond = second;
			}
			currentCount++;
			
			// measure read throughput
			n++;
			if (System.currentTimeMillis() > t + 1000) {
				System.out.println(n + " lines read in the last second");
				n = 0;
				t = System.currentTimeMillis();
			}
		}
		
		ps.close();
		br.close();
		
		System.out.println("Max Reports per Second: " + maxReportsPerSecond + " (second: " + maxSecond + ")");
	}
	
	private static int getSecond(String line) {
		return Integer.parseInt(
			line.substring(
				line.indexOf(',') + 1, 
				line.indexOf(
					',', 
					line.indexOf(',') + 1
				)
			)
		);
	}

}
