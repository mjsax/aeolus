/*
 * #!
 * %
 * Copyright (C) 2014 - 2016 Humboldt-Universität zu Berlin
 * %
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
 * #_
 */
package de.hub.cs.dbis.aeolus.testUtils;

import java.util.Comparator;
import java.util.List;





/**
 * @author mjsax
 */
public class TimestampComperator implements Comparator<List<Object>> {
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
		
		return 0;
	}
}
