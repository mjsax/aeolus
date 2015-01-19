package storm.lrb.tools;

/*
 * #%L
 * lrb
 * $Id:$
 * $HeadURL:$
 * %%
 * Copyright (C) 2014 - 2015 Humboldt-Universität zu Berlin
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

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
