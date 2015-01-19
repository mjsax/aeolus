package storm.lrb.model;

//import com.citusdata.elven.linearRoad.FullSegment;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.HashMap;
import java.util.Map;
import storm.lrb.bolt.SegmentIdentifier;


// Helper class that computes statistics associated with one segment, over one
// minute.
class MinuteStatistics {
  @Override
	public String toString() {
		return " [vehicleSpeeds=" + vehicleSpeeds
				+ ", speedAverage=" + speedAverage + "]";
	}

private final Map    vehicleSpeeds = new HashMap();
  private double speedAverage; // rolling average for vehicles in this segment

  
  protected synchronized void addVehicleSpeed(int vehicleId, int vehicleSpeed) {
    double cumulativeSpeed = speedAverage * vehicleSpeeds.size();
    if (vehicleSpeeds.containsKey(vehicleId)) {
      int prevVehicleSpeed = ((Integer)
	      vehicleSpeeds.get(vehicleId)); 
      cumulativeSpeed -= prevVehicleSpeed;
      cumulativeSpeed += (prevVehicleSpeed+vehicleSpeed)/2.0;
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
}


public class SegmentStatistics implements Serializable {
  @Override
	public String toString() {
		return "SegmentStatistics [segmentsMinutes=" + segmentsMinutes + "]";
	}

/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
// The number of minutes in the past for which we keep vehicle statistics.
  protected static final int MAX_HISTORICAL_MINS = 10;
  protected static final int TOTAL_MINS = 200;

  // We need to keep statistics for each segment. For this, we use a Map where
  // the key is a segment. The value is a list keeping time-based statistics.
  private final ConcurrentMap<SegmentIdentifier, List<MinuteStatistics>> segmentsMinutes = new ConcurrentHashMap();

  private List minutesListGetAndInit(SegmentIdentifier segment, 
                                     ConcurrentMap segmentMinutes) {

    List minutes = (List) segmentsMinutes.get(segment);
    if (minutes == null) {
      List newMinutes = new ArrayList(Collections.nCopies(TOTAL_MINS, null));
      minutes = (List) segmentsMinutes.putIfAbsent(segment, newMinutes);
      if (minutes == null) {
        minutes = newMinutes;
      }
    }

    return minutes;
  }

  
  public int getSegmentCount(){
	  return segmentsMinutes.size();
  }
  
 public Set<SegmentIdentifier> getXsdList(){
	  return segmentsMinutes.keySet();
	  
  }
  
  public void addVehicleSpeed(int minute, SegmentIdentifier xsd, 
                                 int vid, int speed) {

	  //System.out.println("segmentstats: "+minute + "xsd: "+xsd+" spd: "+speed);
    // We need to keep segment statistics for each minute. For this, we use a
    // List where each index represents statistics for one minute.
    List<MinuteStatistics> minutes = minutesListGetAndInit(xsd, segmentsMinutes);

    MinuteStatistics minuteStatistics;
    synchronized(minutes) {
      minuteStatistics = (MinuteStatistics) minutes.get(minute);

      if (minuteStatistics == null) {
        minuteStatistics = new MinuteStatistics();
        minutes.set(minute, minuteStatistics);

        // Switching over to a new minute indicates that we may need to clear
        // historical statistics. 
        int oldMinute = minute-MAX_HISTORICAL_MINS;
        if (oldMinute >= 0) {
          minutes.set(oldMinute, null);
        }
      }
    }

    minuteStatistics.addVehicleSpeed(vid, speed);
  }

  private MinuteStatistics findMinuteStatistics(int minute, SegmentIdentifier xsd) { 
    List minutes = minutesListGetAndInit(xsd, segmentsMinutes);

    MinuteStatistics minuteStatistics;
    synchronized(minutes) {
      minuteStatistics = (MinuteStatistics) minutes.get(minute);
      if (minuteStatistics == null) {
        minuteStatistics = new MinuteStatistics();
        minutes.set(minute, minuteStatistics);
      }
    }

    return minuteStatistics;
  }

  public int vehicleCount(int minute, SegmentIdentifier xsd) {
    MinuteStatistics minuteStatistics = findMinuteStatistics(minute, xsd);
    return minuteStatistics.vehicleCount();
  }

  public double speedAverage(int minute, SegmentIdentifier xsd) {
    MinuteStatistics minuteStatistics = findMinuteStatistics(minute, xsd);
    return minuteStatistics.speedAverage();
  }
}