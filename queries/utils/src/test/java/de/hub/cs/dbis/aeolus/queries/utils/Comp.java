package de.hub.cs.dbis.aeolus.queries.utils;

import java.util.Comparator;
import java.util.List;





/**
 * @author Matthias J. Sax
 */
class Comp implements Comparator<List<Object>> {
	@Override
	public int compare(List<Object> o1, List<Object> o2) {
		long first = ((Long)o1.get(0));
		long second = ((Long)o2.get(0));
		if(first < second) {
			return -1;
		}
		
		if(second < first) {
			return 1;
		}
		
		return 0;
	}
}
