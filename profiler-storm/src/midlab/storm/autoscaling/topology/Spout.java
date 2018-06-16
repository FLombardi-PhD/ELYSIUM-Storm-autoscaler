package midlab.storm.autoscaling.topology;

import java.util.ArrayList;

/**
 * Storm Spout
 * @author Federico Lombardi - Sapienza University of Rome
 *
 */
public class Spout extends Component{

	/** dimension of the sliding window **/
	public int WINDOW_LENGTH = 10;
	
	private ArrayList<Double> window;
	private ArrayList<Long> currentMinuteList;
	
	/**
	 * 
	 * @param name
	 */
	public Spout(String name, int parallelism){
		super(name, parallelism);
		window = new ArrayList<Double>();
		currentMinuteList = new ArrayList<Long>();
	}
	
	/**
	 * 
	 * @return
	 */
	public ArrayList<Double> getWindow(){
		return this.window;
	}
	
	/**
	 * 
	 * @return
	 */
	public ArrayList<Long> getCurrentMinuteList(){
		return this.currentMinuteList;
	}
	
	/**
	 * 
	 * @param a
	 */
	public void setWindow(ArrayList<Double> a){
		this.window = a;
	}
	
	/**
	 * 
	 * @param a
	 */
	public void setCurrentMinuteList(ArrayList<Long> a){
		this.currentMinuteList = a;
	}
	
	/**
	 * 
	 */
	public boolean isBolt(){
		return false;
	}
	
	/**
	 * 
	 */
	public boolean isSpout(){
		return true;
	}
	
}
