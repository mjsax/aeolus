package storm.lrb.model;

import java.io.Serializable;





/**
 * Object that holds the latest average speed and number of vehicles of the given minute
 */
public class NovLav implements Serializable {
	
	private static final long serialVersionUID = 1L;
	private final int nov;
	private final double lav;
	private final int minute;
	
	public NovLav(int cnt, double lav, int minute) {
		this.nov = cnt;
		this.lav = lav;
		this.minute = minute;
	}
	
	public NovLav() {
		this.nov = 0;
		this.lav = 0.0;
		this.minute = 0;
	}
	
	public double getLav() {
		return this.lav;
	}
	
	public int getMinute() {
		return this.minute;
	}
	
	public int getNov() {
		return this.nov;
	}
	
	public boolean isEmpty() {
		return this.nov == 0;
	}
	
	@Override
	public String toString() {
		return "NovLav [nov=" + this.nov + ", lav=" + this.lav + ", min=" + this.minute + "]";
	}
}
