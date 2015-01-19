package storm.lrb.tools;

import java.util.Iterator;
import java.util.List;
import org.apache.commons.lang3.tuple.Triple;

/**
 * Helper class contains useful methods.
 *
 */
public class Helper {

	public static String getXDfromXSD(String xsd) {
		String[] tmp = xsd.split("-");
		if (tmp.length == 3)
			return tmp[0] + "-" + tmp[2];
		else
			return "";
	}

	public static Integer getDirFromXSD(Triple<Integer,Integer,Integer> xsd) {
		return xsd.getRight();
	}

	public static Integer getXwayFromXSD(Triple<Integer,Integer,Integer> xsd) {
		return xsd.getLeft();
	}

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
