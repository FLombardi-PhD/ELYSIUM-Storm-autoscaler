
public class TestForLatencyThroughputCalculator {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		String entry = "1414667658228,626";

		System.out.println("TS: " + Long.parseLong(entry.substring(0, entry.indexOf(','))));
		System.out.println("latency: " + Integer.parseInt(entry.substring(entry.indexOf(',') + 1)));
	}

}
