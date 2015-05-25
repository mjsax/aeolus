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
/*
 * Copyright 2015 Humboldt-Universität zu Berlin.
 *
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
 */
package storm.lrb.tools;

/*
 * #%L
 * lrb-testutils
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
import java.util.Random;
import storm.lrb.bolt.SegmentIdentifier;
import storm.lrb.model.PosReport;





/**
 * 
 * @author richter
 */
public class EntityHelper {
	
	public static final int POS_REPORT_MAX_SPEED_DEFAULT = 250;
	
	/**
	 * Creates instances of {@link PosReport} for running cars, i.e. there're no instances of stopped cars created (not
	 * even randomly). See {@link #createPosReport(java.util.Random, int, int, int)} for details.
	 * 
	 * @param random
	 * @param vehicleID
	 * @return an instance of {@link PosReport} simulating a running car
	 */
	public static PosReport createPosReport(Random random, int vehicleID) {
		return createPosReport(random, vehicleID, 1, POS_REPORT_MAX_SPEED_DEFAULT);
	}
	
	/**
	 * creates a {@link PosReport} like {@link #createPosReport(long, java.util.Random, int, int, int) } does, except for
	 * the {@code time} and {@code segment} which is randomly generated.
	 * 
	 * @param random
	 * @param vehicleID
	 * @param minSpeed
	 * @param maxSpeed
	 * @return
	 */
	public static PosReport createPosReport(Random random, int vehicleID, int minSpeed, int maxSpeed) {
		int time = (int)(System.currentTimeMillis() - random.nextDouble() * System.currentTimeMillis());
		int segment = (int)(random.nextDouble() * 100000); // set max. value to increase readability;
		return createPosReport(time, segment, random, vehicleID, minSpeed, maxSpeed);
	}
	
	/**
	 * Creates instances of {@link PosReport} which can be used in tests. Values of properties are chosen randomly
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
	 * @return an instance of {@link PosReport}
	 */
	/*
	 * internal implementation notes: - pass vehicleID because it is shared in Accident
	 */
	public static PosReport createPosReport(long time, int segment, Random random, int vehicleID, int minSpeed, int maxSpeed) {
		int currentSpeed = (int)(minSpeed + (maxSpeed - minSpeed) * random.nextDouble());// set max. value to increase
		// readability
		int direction = SegmentIdentifier.DIRECTION_EASTBOUND;
		SegmentIdentifier segmentIdentifier = new SegmentIdentifier(1, // xWay
			segment, direction);
		int position = 1;
		StopWatch timer = new StopWatch(System.currentTimeMillis());
		PosReport posReport = new PosReport(time, vehicleID, currentSpeed, 1, // xWay
			1,// lane
			Constants.DIRECTION_EASTBOUND, // direction
			segmentIdentifier, position, timer);
		return posReport;
	}
	
	private EntityHelper() {}
}
