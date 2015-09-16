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

import de.hub.cs.dbis.lrb.types.PositionReport;
import de.hub.cs.dbis.lrb.util.Constants;
import java.util.Random;





/**
 * @author richter
 * @author mjsax
 */
public class EntityHelper {
	
	/**
	 * Creates instances of {@link PositionReport} for running cars, i.e. there're no instances of stopped cars created
	 * (not even randomly). See {@link #createPosReport(java.util.Random, Integer, int, int)} for details.
	 * 
	 * @param random
	 * @param vehicleID
	 * 
	 * @return an instance of {@link PositionReport} simulating a running car
	 */
	public static PositionReport createPosReport(Random random, Integer vehicleID) {
		return createPosReport(random, vehicleID, 1, Constants.NUMBER_OF_SPEEDS);
	}
	
	/**
	 * creates a {@link PositionReport} like {@link #createPosReport(Short, Short, java.util.Random, Integer, int, int) }
	 * does, except for the {@code time} and {@code segment} which is randomly generated.
	 * 
	 * @param random
	 * @param vehicleID
	 * @param minSpeed
	 * @param maxSpeed
	 * 
	 * @return
	 */
	public static PositionReport createPosReport(Random random, Integer vehicleID, int minSpeed, int maxSpeed) {
		Short time = new Short((short)random.nextInt(Constants.NUMBER_OF_SECONDS));
		Short segment = new Short((short)(random.nextDouble() * Constants.NUMBER_OF_SEGMENT));
		return createPosReport(time, segment, random, vehicleID, minSpeed, maxSpeed);
	}
	
	/**
	 * Creates instances of {@link PositionReport} which can be used in tests. Values of properties are chosen randomly
	 * within bounds which are there to ensure a certain readability in trace logging and/or debugging.
	 * 
	 * @param time
	 *            the {@code time} property in seconds of the report (pay attention to bolt which are minute
	 * @param segment
	 *            the {@code segment} property of the report
	 * @param random
	 *            the instance of {@link Random} used to create property values within bounds
	 * @param vehicleID
	 *            a value for the vehicleID property
	 * @param minSpeed
	 *            returned instances won't have less than the specified value set as value for the speed property
	 * @param maxSpeed
	 *            returned instances won't have more than the specified value set as value for the speed property (use
	 *            in conjunction with {@code minSpeed} to simulate a stopped car
	 * @return an instance of {@link PositionReport}
	 */
	/*
	 * internal implementation notes: - pass vehicleID because it is shared in Accident
	 */
	public static PositionReport createPosReport(Short time, Short segment, Random random, Integer vehicleID, int minSpeed, int maxSpeed) {
		// set max. value to increase
		Integer speed = new Integer((int)(minSpeed + (maxSpeed - minSpeed) * random.nextDouble()));
		Integer position = new Integer(1);
		
		return new PositionReport(time, vehicleID, speed, new Integer(1), // xWay
			new Short((short)1),// lane
			Constants.EASTBOUND, // direction
			segment, position);
	}
	
	private EntityHelper() {}
	
}
