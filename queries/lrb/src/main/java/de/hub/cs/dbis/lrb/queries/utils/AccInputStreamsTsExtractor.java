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
package de.hub.cs.dbis.lrb.queries.utils;

import backtype.storm.tuple.Tuple;
import de.hub.cs.dbis.aeolus.utils.TimeStampExtractor;
import de.hub.cs.dbis.aeolus.utils.TimestampMerger;
import de.hub.cs.dbis.lrb.types.PositionReport;
import de.hub.cs.dbis.lrb.types.internal.AccidentTuple;





/**
 * {@link AccInputStreamsTsExtractor} extracts the minute number of the input tuples as timestamps for a
 * {@link TimestampMerger}.<br />
 * <br />
 * It expects {@link PositionReport} tuples for input stream {@link TopologyControl#POSITION_REPORTS_STREAM_ID}. Tuples
 * from all other input streams are expected to be of type {@link AccidentTuple}.
 * 
 * @author mjsax
 */
public class AccInputStreamsTsExtractor implements TimeStampExtractor<Tuple> {
	private static final long serialVersionUID = -8976869891555736418L;
	
	@Override
	public long getTs(Tuple tuple) {
		if(tuple.getSourceStreamId().equals(TopologyControl.POSITION_REPORTS_STREAM_ID)) {
			return tuple.getShort(PositionReport.TIME_IDX).longValue();
		} else {
			return tuple.getShort(AccidentTuple.TIME_IDX).longValue();
		}
	}
}
