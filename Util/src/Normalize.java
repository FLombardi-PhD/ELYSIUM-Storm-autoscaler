import java.io.BufferedReader;
import java.io.FileReader;
import java.io.PrintStream;


public class Normalize {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		
		/*String s = "1,480,0,2117,28***480,0###PosReport on 0-31-1 [time=480, vid=2117, spd=52, lane=1, dir=1, pos=168013(Created: 482236 Duration: 0 ms, StormTimer: 482s)]###";
		System.out.println(s.substring(0, s.indexOf("*")));
		System.exit(0);*/
		
		String input = args[0];
		String output = args[1];
		
		BufferedReader br = new BufferedReader(new FileReader(input));
		PrintStream ps = new PrintStream(output);
		String line;
		while ((line = br.readLine())!=null) {
			int i = line.indexOf("*");
			if (i==-1)
				i = line.indexOf("#");
			ps.println(line.substring(0, i));
		}
		ps.close();
		br.close();
	}

}
