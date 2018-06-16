package midlab.storm.autoscaling.utility;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Utility functions
 * @author Federico Lombardi - Sapienza University of Rome
 *
 */
public class Utils {

	/**
	 * Print an array
	 * @param a
	 * @return
	 */
	public static String printArray(Object[] a){
		String s = "";
		for(int i=0; i<a.length; ++i){
			s += (a[i]);
			if(i<a.length-1) s += "  ";
		}
		return s;
	}
	
	/**
	 * Compute the avg of a given array
	 * @param a
	 * @return
	 */
	public static double avgCalculate(Double[] a){
		double sum = 0.0;
		for(int i=0; i<a.length; ++i){
			sum += a[i];
		}
		return (double)sum/(double)a.length;
	}
	
	/**
	 * Convert a given timestamp in a normalized date (without seconds) of type: day,month,date,hour,minutes
	 * @param timestamp
	 * @return
	 */
	public static String convertTimestampInNormalizedDate(long timestamp){
		//Date date = new Date(timestamp*1000);
		Date date = new Date(timestamp); //TODO: da provare
		
		SimpleDateFormat sdf = new SimpleDateFormat("EEE,d,MMM,HH,mm");
		String formattedDate = sdf.format(date);
		
		String[] dateArr = formattedDate.split(",");
		String dayS = dateArr[0];
		String dateS = dateArr[1];
		
		@SuppressWarnings("unused")
		String monthS = dateArr[2];
		
		String hourS = dateArr[3];
		String minS = dateArr[4];
		
		double normalizedDay = getNormalizedDayFromStringDay(dayS);
		double normalizedDate = Double.parseDouble(dateS)/30.0;
		
	    Calendar cal = Calendar.getInstance();
	    cal.setTime(date);
	    int month = cal.get(Calendar.MONTH);
		double normalizedMonth = (double)month/12.0;  //Double.parseDouble(monthS)/12.0;
		
		double normalizedHour = Double.parseDouble(hourS)/23.0;
		double normalizedMinutes = Double.parseDouble(minS)/59.0;
		
		String normalizedDateS = ""+normalizedDay+","+normalizedMonth+","+normalizedDate+","+normalizedHour+","+normalizedMinutes;
		return normalizedDateS;
	}
	
	/**
	 * Return a double indicating the normalized value in the interval [0.0;1.0] of a weekly day (sun=0, mon=1, .., sat=6)
	 * @param day 3 char representation in italian or english (e.g. sun|dom, mon|lun, etc..)
	 * @return
	 */
	public static double getNormalizedDayFromStringDay(String day){
		if(day.equalsIgnoreCase("sun") || day.equalsIgnoreCase("dom")) return 0.0/6.0; // sun=0
		if(day.equalsIgnoreCase("mon") || day.equalsIgnoreCase("lun")) return 1.0/6.0; // mon=0.1666
		if(day.equalsIgnoreCase("tue") || day.equalsIgnoreCase("mar")) return 2.0/6.0; // tue=0.3333
		if(day.equalsIgnoreCase("wed") || day.equalsIgnoreCase("mer")) return 3.0/6.0; // wed=0.5
		if(day.equalsIgnoreCase("thu") || day.equalsIgnoreCase("gio")) return 4.0/6.0; // thu=0.6666
		if(day.equalsIgnoreCase("fri") || day.equalsIgnoreCase("ven")) return 5.0/6.0; // fri=0.8333
		if(day.equalsIgnoreCase("sat") || day.equalsIgnoreCase("sab")) return 6.0/6.0; // sat=1.0
		return 0.0;
	}
}
