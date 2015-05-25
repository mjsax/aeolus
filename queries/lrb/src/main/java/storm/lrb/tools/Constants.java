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
	
	private Constants() {}
}
