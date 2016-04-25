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
package de.hub.cs.dbis.lrb.queries;

import java.util.HashMap;

import de.hub.cs.dbis.lrb.queries.utils.TopologyControl;





/**
 * {@link OperatorParallelism} contains the {@code parallelism_hint} for each LRB operator. The default value is 1.
 * 
 * @author mjsax
 */
final class OperatorParallelism {
	/** Internal map that holds the {@code parallelism_hint} for each LRB operator. */
	private final static HashMap<String, Integer> parallelism = new HashMap<String, Integer>();
	/** Default value of {@code 1} if {@code parallelism_hint} was not set. */
	private final static Integer defaultParallelism = new Integer(1);
	
	
	
	/**
	 * Returns the parallelism for operator with ID {@code name}.
	 * 
	 * @param name
	 *            The ID of the operator (see {@link TopologyControl}) for valid IDs).
	 * 
	 * @return The parallelism for operator {@code name}.
	 */
	public static Integer get(String name) {
		final Integer p = parallelism.get(name);
		if(p == null) {
			return defaultParallelism;
		}
		
		return p;
	}
	
	/**
	 * Sets the parallelism for operator with ID {@code name}.
	 * 
	 * @param name
	 *            The ID of the operator (see {@link TopologyControl}) for valid IDs).
	 * @param parallelismHint
	 *            The parallelism for operator {@code name}
	 * 
	 * @throws IllegalArgumentException
	 *             if {@code name == null} or {@code parallelism_hint < 1}
	 */
	public static void set(String name, int parallelismHint) throws IllegalArgumentException {
		if(name == null) {
			throw new IllegalArgumentException("<name> cannot be null.");
		}
		if(parallelismHint < 1) {
			throw new IllegalArgumentException("<parallelism_hint> must be at least 1.");
		}
		parallelism.put(name, new Integer(parallelismHint));
	}
}
