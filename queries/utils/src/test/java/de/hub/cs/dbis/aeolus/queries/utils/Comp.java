package de.hub.cs.dbis.aeolus.queries.utils;

import java.util.Comparator;
import java.util.List;
import java.util.Random;





/**
 * @author Matthias J. Sax
 */
class Comp implements Comparator<List<Object>> {
	private Random rr;
	
	public Comp(Random rr) {
		this.rr = rr;
	}
	
	@Override
	public int compare(List<Object> o1, List<Object> o2) {
		long first = ((Long)o1.get(0)).longValue();
		long second = ((Long)o2.get(0)).longValue();
		if(first < second) {
			return -1;
		}
		
		if(second < first) {
			return 1;
		}
		
		return -1 + this.rr.nextInt(3);
	}
	
}
