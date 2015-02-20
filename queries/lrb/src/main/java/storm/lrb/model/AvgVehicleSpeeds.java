package storm.lrb.model;

import java.util.HashMap;
import java.util.Map;

/**
 *
 * Class to compute average speeds for vehicles (for one minute in one segment)
 *
 */
public class AvgVehicleSpeeds {

    /**
     * holds average speed for each vehicle
     */
    private final Map<Integer, Integer> avgsPerVehicle = new HashMap<Integer, Integer>();
    /**
     * holds overall average speed for all vehicles
     */
    private double speedAverage;

    /**
     * register speed of vehicle
     *
     * @param vehicleId
     * @param vehicleSpeed
     */
    public synchronized void addVehicleSpeed(int vehicleId, int vehicleSpeed) {

        double cumulativeSpeed = speedAverage * avgsPerVehicle.size();

        if (avgsPerVehicle.containsKey(vehicleId)) {
            int prevVehicleSpeed = avgsPerVehicle.get(vehicleId);
            cumulativeSpeed -= prevVehicleSpeed;
            cumulativeSpeed += (prevVehicleSpeed + vehicleSpeed) / 2.0;
        } else {
            avgsPerVehicle.put(vehicleId, vehicleSpeed);
            cumulativeSpeed += vehicleSpeed;
        }

        speedAverage = cumulativeSpeed / avgsPerVehicle.size();
    }

    public synchronized double speedAverage() {
        return speedAverage;
    }

    public synchronized int vehicleCount() {
        return avgsPerVehicle.size();
    }

    @Override
    public String toString() {
        return " [avgsPerVehicle=" + avgsPerVehicle + ", speedAverage="
                + speedAverage + "]";
    }

}
