package midlab.storm.autoscaling.topology;

/**
 * Storm Bolt
 * @author Federico Lombardi - Sapienza University of Rome
 *
 */
public class Bolt extends Component{
	
	public Bolt(String name, int parallelism){
		super(name, parallelism);
	}
	
	public boolean isBolt(){
		return true;
	}
	
	public boolean isSpout(){
		return false;
	}
		
	
}
