package storm.lrb.tools;

import java.util.Iterator;
import java.util.List;





/**
 * Helper class contains useful methods.
 * 
 */
public class Helper {
	
	public static String readable(List<String> fields) {
		StringBuilder tmp = new StringBuilder();
		for(Iterator<String> iterator = fields.iterator(); iterator.hasNext();) {
			String string = iterator.next();
			if(iterator.hasNext()) {
				tmp.append(string).append("-");
			} else {
				tmp.append(string);
			}
		}
		return tmp.toString();
	}
	
	private Helper() {}
}
