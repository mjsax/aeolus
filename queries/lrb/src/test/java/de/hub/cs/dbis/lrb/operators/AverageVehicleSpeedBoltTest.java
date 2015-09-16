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
package de.hub.cs.dbis.lrb.operators;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import storm.lrb.TopologyControl;
import storm.lrb.tools.EntityHelper;
import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;
import de.hub.cs.dbis.aeolus.testUtils.AbstractBoltTest;
import de.hub.cs.dbis.aeolus.testUtils.TestDeclarer;
import de.hub.cs.dbis.aeolus.testUtils.TestOutputCollector;
import de.hub.cs.dbis.lrb.types.AvgVehicleSpeedTuple;
import de.hub.cs.dbis.lrb.types.PositionReport;
import de.hub.cs.dbis.lrb.types.SegmentIdentifier;
import de.hub.cs.dbis.lrb.util.Constants;





/**
 * @author richter
 * @author mjsax
 */
public class AverageVehicleSpeedBoltTest extends AbstractBoltTest {
	
	@Test
	public void testExecute() {
		TestOutputCollector collector = new TestOutputCollector();
		
		AverageVehicleSpeedBolt bolt = new AverageVehicleSpeedBolt();
		bolt.prepare(null, null, new OutputCollector(collector));
		
		final Tuple tuple = mock(Tuple.class);
		
		
		
		int vehicleID0, vehicleID1, vehicleID2;
		vehicleID0 = (int)(this.r.nextDouble() * Constants.NUMBER_OF_VIDS);
		do {
			vehicleID1 = (int)(this.r.nextDouble() * Constants.NUMBER_OF_VIDS);
		} while(vehicleID1 == vehicleID0);
		do {
			vehicleID2 = (int)(this.r.nextDouble() * Constants.NUMBER_OF_VIDS);
		} while(vehicleID2 == vehicleID1 || vehicleID2 == vehicleID0);
		
		short segment = (short)(this.r.nextDouble() * Constants.NUMBER_OF_SEGMENT);
		
		
		
		short time0 = (short)(1 + this.r.nextInt(60));
		PositionReport posReport0Stopped = EntityHelper.createPosReport(new Short(time0), new Short(segment), this.r,
			new Integer(vehicleID0), 0, // minSpeed
			0 // maxSpeed
			);
		when(tuple.getValues()).thenReturn(posReport0Stopped);
		
		bolt.execute(tuple);
		
		assertEquals(1, collector.acked.size());
		assertEquals(0, collector.output.size()); // one tuple doesn't trigger emission
		
		
		
		short time1 = (short)(time0 + 60); // step to next minute
		PositionReport posReport1Stopped = EntityHelper.createPosReport(new Short(time1), new Short(segment), this.r,
			new Integer(vehicleID0), 0, // minSpeed
			0 // maxSpeed
			);
		when(tuple.getValues()).thenReturn(posReport1Stopped);
		
		bolt.execute(tuple);
		
		assertEquals(2, collector.acked.size());
		assertEquals(1, collector.output.size()); // should write to one stream only
		// second tuple with another time should have triggered emission after one minute
		assertEquals(1, collector.output.get(Utils.DEFAULT_STREAM_ID).size());
		
		AvgVehicleSpeedTuple result = (AvgVehicleSpeedTuple)collector.output.get(Utils.DEFAULT_STREAM_ID).get(0);
		assertEquals(new SegmentIdentifier(posReport1Stopped), new SegmentIdentifier(result));
		assertEquals(time1 / 60, result.getMinute().shortValue());
		assertEquals(0, result.getAvgSpeed().intValue());
		
		
		
		short time2 = (short)(time1 + 60); // step to next minute
		// three cars
		int speed2 = this.r.nextInt(Constants.NUMBER_OF_SPEEDS);
		int speed3 = this.r.nextInt(Constants.NUMBER_OF_SPEEDS);
		int speed4 = this.r.nextInt(Constants.NUMBER_OF_SPEEDS);
		
		PositionReport posReport2Running = EntityHelper.createPosReport(new Short(time1), new Short(segment), this.r,
			new Integer(vehicleID1), speed2, // minSpeed
			speed2 // maxSpeed
			);
		when(tuple.getValues()).thenReturn(posReport2Running);
		
		bolt.execute(tuple);
		assertEquals(3, collector.acked.size());
		
		PositionReport posReport3Running = EntityHelper.createPosReport(new Short(time1), new Short(segment), this.r,
			new Integer(vehicleID1), speed3, // minSpeed
			speed3 // maxSpeed
			);
		when(tuple.getValues()).thenReturn(posReport3Running);
		
		bolt.execute(tuple);
		assertEquals(4, collector.acked.size());
		
		PositionReport posReport4Running = EntityHelper.createPosReport(new Short(time2), new Short(segment), this.r,
			new Integer(vehicleID2), speed4, // minSpeed
			speed4 // maxSpeed
			);
		when(tuple.getValues()).thenReturn(posReport4Running);
		
		bolt.execute(tuple);
		
		assertEquals(5, collector.acked.size());
		assertEquals(1, collector.output.size()); // one stream only
		assertEquals(3, collector.output.get(Utils.DEFAULT_STREAM_ID).size());
		
		for(int i = 1; i < 3; ++i) {
			result = (AvgVehicleSpeedTuple)collector.output.get(Utils.DEFAULT_STREAM_ID).get(i);
			assertEquals(new SegmentIdentifier(posReport1Stopped), new SegmentIdentifier(result));
			// average update only occurs after emission
			if(result.getVid().intValue() == vehicleID0) {
				assertEquals(0, result.getAvgSpeed().intValue());
			} else {
				assertEquals(result.getVid().intValue(), vehicleID1);
				assertEquals((speed2 + speed3) / 2, result.getAvgSpeed().intValue());
			}
			assertEquals(time2 / 60, result.getMinute().shortValue());
		}
	}
	
	@Test
	public void testDeclareOutputFields() {
		AverageVehicleSpeedBolt bolt = new AverageVehicleSpeedBolt();
		
		TestDeclarer declarer = new TestDeclarer();
		bolt.declareOutputFields(declarer);
		
		Assert.assertEquals(1, declarer.streamIdBuffer.size());
		Assert.assertEquals(1, declarer.schemaBuffer.size());
		Assert.assertEquals(1, declarer.directBuffer.size());
		
		Assert.assertNull(declarer.streamIdBuffer.get(0));
		Assert.assertEquals(new Fields(TopologyControl.VEHICLE_ID_FIELD_NAME, TopologyControl.MINUTE_FIELD_NAME,
			TopologyControl.XWAY_FIELD_NAME, TopologyControl.SEGMENT_FIELD_NAME, TopologyControl.DIRECTION_FIELD_NAME,
			TopologyControl.AVERAGE_VEHICLE_SPEED_FIELD_NAME).toList(), declarer.schemaBuffer.get(0).toList());
		Assert.assertEquals(new Boolean(false), declarer.directBuffer.get(0));
	}
	
}
