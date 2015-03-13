package storm.lrb.model;

/**
 * helper class to compute the current minute of given second
 * 
 */
public class Time {
	
	public static int getMinute(int sec) {
		return ((sec / 60) + 1);
	}
	
	private Time() {}
	
}
