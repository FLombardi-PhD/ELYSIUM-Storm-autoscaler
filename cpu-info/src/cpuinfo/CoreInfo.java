package cpuinfo;

public class CoreInfo {
	
	public static final String ID_PROPERTY = "processor";
	public static final String MODEL_NAME_PROPERTY = "model name";
	public static final String SPEED_PROPERTY = "cpu MHz";

	/**
	 * core ID
	 */
	private final int id;
	
	/**
	 * core model
	 */
	private final String modelName;
	
	/**
	 * core speed in Hz
	 */
	private final long speed;
	
	public CoreInfo(int id, String modelName, long speed) {
		this.id = id;
		this.modelName = modelName;
		this.speed = speed;
	}

	public int getId() {
		return id;
	}

	public String getModelName() {
		return modelName;
	}

	/**
	 * @return core speed in Hz
	 */
	public long getSpeed() {
		return speed;
	}
	
	@Override
	public String toString() {
		return "ID: " + id + ", model: " + modelName + ", speed: " + speed + " Hz";
	}
}
