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
package de.hub.cs.dbis.lrb.operators;

import backtype.storm.tuple.Tuple;
import de.hub.cs.dbis.aeolus.sinks.AbstractFileOutputBolt;





/**
 * @author mjsax
 */
public class SpoutDataFileOutputBolt extends AbstractFileOutputBolt {
	private static final long serialVersionUID = 412459844575730202L;
	
	@Override
	protected String tupleToString(Tuple t) {
		return t.getLong(0) + "," + t.getString(1) + "\n";
	}
	
}
