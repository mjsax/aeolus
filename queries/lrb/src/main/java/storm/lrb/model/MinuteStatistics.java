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
package storm.lrb.model;

/*
 * #%L
 * lrb
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

import java.util.HashMap;
import java.util.Map;

/**
 * Helper class that computes statistics associated with one segment, over one
 * minute.
 *
 * @author richter
 */
public class MinuteStatistics {

    private final Map<Integer, Integer> vehicleSpeeds = new HashMap<Integer, Integer>();
    private double speedAverage; // rolling average for vehicles in this segment

    protected synchronized void addVehicleSpeed(int vehicleId, int vehicleSpeed) {
        double cumulativeSpeed = speedAverage * vehicleSpeeds.size();
        if (vehicleSpeeds.containsKey(vehicleId)) {
            int prevVehicleSpeed = vehicleSpeeds.get(vehicleId);
            cumulativeSpeed -= prevVehicleSpeed;
            cumulativeSpeed += (prevVehicleSpeed + vehicleSpeed) / 2.0;
        } else {
            vehicleSpeeds.put(vehicleId, vehicleSpeed);
            cumulativeSpeed += vehicleSpeed;
        }

        speedAverage = cumulativeSpeed / vehicleSpeeds.size();
    }

    protected synchronized double speedAverage() {
        return speedAverage;
    }

    protected synchronized int vehicleCount() {
        return vehicleSpeeds.size();
    }

    @Override
    public String toString() {
        return " [vehicleSpeeds=" + vehicleSpeeds
                + ", speedAverage=" + speedAverage + "]";
    }
}
