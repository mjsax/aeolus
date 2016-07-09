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
package de.hub.cs.dbis.lrb.queries;

import java.io.IOException;

import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import de.hub.cs.dbis.aeolus.sinks.FileFlushSinkBolt;
import de.hub.cs.dbis.aeolus.utils.TimestampMerger;
import de.hub.cs.dbis.lrb.operators.StoppedCarsBolt;
import de.hub.cs.dbis.lrb.queries.utils.TopologyControl;
import de.hub.cs.dbis.lrb.types.PositionReport;





/**
 * {@link StoppedCarsSubquery} assembles the "stopped cars" subquery that must detect stopped cars (for non-exit lanes).
 * 
 * @author mjsax
 */
public class StoppedCarsSubquery extends AbstractQuery {
	private OptionSpec<String> output;
	
	
	
	public StoppedCarsSubquery() {
		this(true);
	}
	
	public StoppedCarsSubquery(boolean required) {
		this.output = parser.accepts("stopped-output", "Bolt local path to write stopped cars.").withRequiredArg()
			.describedAs("file").ofType(String.class);
		
		if(required) {
			this.output = ((ArgumentAcceptingOptionSpec<String>)this.output).required();
		}
	}
	
	
	
	/**
	 * {@inheritDoc}
	 * 
	 * Does not have any intermediate output. Parameter {@code intermediateOutput} is void.
	 */
	@Override
	protected void addBolts(TopologyBuilder builder, OptionSet options) {
		try {
			builder
				.setBolt(TopologyControl.STOPPED_CARS_BOLT_NAME,
					new TimestampMerger(new StoppedCarsBolt(), PositionReport.TIME_IDX),
					OperatorParallelism.get(TopologyControl.STOPPED_CARS_BOLT_NAME))
				.fieldsGrouping(TopologyControl.SPLIT_STREAM_BOLT_NAME, TopologyControl.POSITION_REPORTS_STREAM_ID,
					new Fields(TopologyControl.VEHICLE_ID_FIELD_NAME))
				.allGrouping(TopologyControl.SPLIT_STREAM_BOLT_NAME, TimestampMerger.FLUSH_STREAM_ID);
		} catch(IllegalArgumentException e) {
			// happens if complete LRB is assembled, because "stopped cars" is part of "accident query" and
			// "toll query"
			if(e.getMessage().equals("Bolt has already been declared for id " + TopologyControl.STOPPED_CARS_BOLT_NAME)) {
				/* ignore */
			} else {
				throw e;
			}
		}
		
		if(options.has(this.output)) {
			builder.setBolt("stopped-sink", new FileFlushSinkBolt(options.valueOf(this.output)))
				.localOrShuffleGrouping(TopologyControl.STOPPED_CARS_BOLT_NAME);
		}
	}
	
	
	
	public static void main(String[] args) throws IOException, InvalidTopologyException, AlreadyAliveException {
		new StoppedCarsSubquery().parseArgumentsAndRun(args);
	}
	
}
