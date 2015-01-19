package storm.lrb.tools;

import java.util.Iterator;
import java.util.List;
import org.apache.commons.lang3.tuple.Triple;

/**
 * Helper class contains useful methods.
 *
 */
public class Helper {

	public static String readable(List<String> fields) {
		StringBuilder tmp = new StringBuilder();
		for (Iterator iterator = fields.iterator(); iterator.hasNext();) {
			String string = (String) iterator.next();
			if(iterator.hasNext()) tmp.append(string+"-");
			else tmp.append(string);
		}
		return tmp.toString();
	}
}
