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

import joptsimple.OptionSet;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;





/**
 * {@link LinearRoad} assembles the {@link AccidentQuery Accident} and the {@link TollQuery Toll} processing queries in
 * a single topology.
 * 
 * * @author mjsax
 */
public class LinearRoad extends AbstractQuery {
	private final AccidentQuery accQuery;
	private final TollQuery tollQuery;
	
	
	
	public LinearRoad() {
		this.accQuery = new AccidentQuery();
		this.tollQuery = new TollQuery();
	}
	
	
	
	@Override
	protected void addBolts(TopologyBuilder builder, OptionSet options) {
		this.accQuery.addBolts(builder, options);
		this.tollQuery.addBolts(builder, options);
	}
	
	
	
	public static void main(String[] args) throws IOException, InvalidTopologyException, AlreadyAliveException {
		new LinearRoad().parseArgumentsAndRun(args);
	}
	
}
