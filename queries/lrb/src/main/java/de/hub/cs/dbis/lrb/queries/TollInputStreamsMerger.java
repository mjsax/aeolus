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
package de.hub.cs.dbis.lrb.queries;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.lrb.TopologyControl;
import backtype.storm.tuple.Tuple;
import de.hub.cs.dbis.aeolus.utils.TimeStampExtractor;
import de.hub.cs.dbis.lrb.operators.TollNotificationBolt;
import de.hub.cs.dbis.lrb.types.PositionReport;
import de.hub.cs.dbis.lrb.types.internal.AccidentTuple;
import de.hub.cs.dbis.lrb.types.internal.CountTuple;
import de.hub.cs.dbis.lrb.types.internal.LavTuple;
import de.hub.cs.dbis.lrb.util.Time;





/**
 * {@link TollInputStreamsMerger} helps to merge the four incoming streams of {@link TollNotificationBolt}.
 * 
 * @author mjsax
 */
class TollInputStreamsMerger implements TimeStampExtractor<Tuple> {
	private static final long serialVersionUID = -234551807946550L;
	private static final Logger LOGGER = LoggerFactory.getLogger(TollInputStreamsMerger.class);
	
	@Override
	public long getTs(Tuple tuple) {
		final String inputStreamId = tuple.getSourceStreamId();
		if(inputStreamId.equals(TopologyControl.POSITION_REPORTS_STREAM_ID)) {
			return Time.getMinute(tuple.getShort(PositionReport.TIME_IDX).longValue());
		} else if(inputStreamId.equals(TopologyControl.ACCIDENTS_STREAM_ID)) {
			return tuple.getShort(AccidentTuple.MINUTE_IDX).longValue();
		} else if(inputStreamId.equals(TopologyControl.CAR_COUNTS_STREAM_ID)) {
			return tuple.getShort(CountTuple.MINUTE_IDX).longValue();
		} else if(inputStreamId.equals(TopologyControl.LAVS_STREAM_ID)) {
			return tuple.getShort(LavTuple.MINUTE_IDX).longValue() - 1;
		} else {
			LOGGER.error("Unknown input stream: '" + inputStreamId + "' for tuple " + tuple);
			throw new RuntimeException("Unknown input stream: '" + inputStreamId + "' for tuple " + tuple);
		}
	}
	
}
