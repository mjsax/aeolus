/*
 * #!
 * %
 * Copyright (C) 2014 - 2015 Humboldt-Universität zu Berlin
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
package storm.lrb.tools;



/**
 * Constants related to the benchmark constraints only.
 * 
 * @author richter
 */
public class Constants {
	
	/**
	 * see p. 483
	 */
	public final static int MAX_NUMBER_OF_POSITIONS = 5280;
	
	/**
	 * @TODO: search in benchmark specification
	 */
	public final static int INITIAL_TOLL = 20;
	
	public static final int DIRECTION_EASTBOUND = 1;
	public static final int DIRECTION_WESTBOUND = 2;
	
	/**
	 * The 0th of the 5 lanes of an express way is the entrance lane
	 * <ref>p.3 LRB paper</ref>.
	 */
	public static final int ENTRANCE_LANE = 0;
	/**
	 * The 4th of the 5 lanes (0-based) of an express way is the exit lane
	 * <ref>p.3 LRB paper</ref>.
	 */
	public static final int EXIT_LANE = 4;
	/**
	 * each express way consists of 100 segments in each direction
	 * <ref>p. 3 LRB paper</ref>.
	 */
	public static final int SEGMENT_COUNT = 100;

	private Constants() {}
}
