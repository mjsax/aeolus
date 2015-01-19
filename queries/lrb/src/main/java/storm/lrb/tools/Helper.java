package storm.lrb.tools;

import java.util.Iterator;
import java.util.List;

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

	public static String getDirFromXSD(String xsd) {
		String[] tmp = xsd.split("-");
		if (tmp.length == 3)
			return tmp[1];
		else
			return "";
	}

	public static String getXwayFromXSD(String xsd) {
		String[] tmp = xsd.split("-");
		if (tmp.length == 3)
			return tmp[0];
		return "";
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
