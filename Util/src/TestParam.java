
public class TestParam {
	
	public static int x = 0;
	
	static {
		x = Integer.parseInt(System.getProperty("test"));
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		System.out.println( System.getProperty("test") );
		
		for (long t = System.currentTimeMillis() ; System.currentTimeMillis() < t + x; );

	}

}
