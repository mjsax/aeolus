package storm.lrb.model;

import java.io.Serializable;

/**
 * Mutable Integer object.
 */
public class MutableInt implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	int value = 1;

	public int increment() {
		return ++value;
	}

	public int get() {
		return value;
	}

	public int add(int summand) {
		return value += summand;
	}

	public void reset() {
		value = 1;
		
	}

	@Override
	public String toString() {
		return ""+value;
	}

	public int decrement() {
		return --value;
	}
}