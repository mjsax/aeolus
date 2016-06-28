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

import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;





/**
 * {@link LinearRoadWithAllIntermediateResults} assembles the {@link AccidentQuery Accident} and the {@link TollQuery
 * Toll} processing queries in a single topology.
 * 
 * * @author mjsax
 */
public class LinearRoadWithAllIntermediateResults extends AbstractQuery {
	
	public static void main(String[] args) throws IOException, InvalidTopologyException, AlreadyAliveException {
		new LinearRoadWithAllIntermediateResults().parseArgumentsAndRun(args, new String[] {
			"accidentNotificationsOutput", "tollNotificationsOutput", "tollAssessmentsOutput"}, new String[] {
			"/data/mjsax/lrb/accidentsOutput.txt", "/data/mjsax/lrb/stoppedOutput.txt",
			"/data/mjsax/lrb/lavOutput.txt", "/data/mjsax/lrb/avgSpdOutput.txt",
			"/data/mjsax/lrb/avgVehicleSpdOutput.txt", "/data/mjsax/lrb/cntOutput.txt"});
	}
	
	/**
	 * {@inheritDoc}
	 * 
	 * Requires three specified outputs for "accident notification", "toll notification" and "toll assessments".
	 * Optional parameter {@code intermediateOutputs} specifies the output of {@link AccidentDetectionSubquery},
	 * {@link StoppedCarsSubquery}, {@link LatestAverageVelocitySubquery}, {@link AverageSpeedSubquery},
	 * {@link AverageVehicleSpeedSubquery}, and {@link CountVehicleSubquery}
	 */
	@Override
	protected void addBolts(TopologyBuilder builder, String[] outputs, String[] intermediateOutputs) {
		if(outputs == null) {
			throw new IllegalArgumentException("Parameter <outputs> must not be null.");
		}
		if(outputs.length < 3) {
			throw new IllegalArgumentException("Parameter <outputs> must provide three values.");
		}
		
		new AccidentQuery().addBolts(builder, new String[] {outputs[0]}, null);
		new TollQuery().addBolts(builder, new String[] {outputs[1], outputs[2]}, intermediateOutputs);
	}
}
