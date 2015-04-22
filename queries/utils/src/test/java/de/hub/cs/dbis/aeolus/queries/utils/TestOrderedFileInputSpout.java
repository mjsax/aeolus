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
package de.hub.cs.dbis.aeolus.queries.utils;

import java.text.ParseException;
import java.util.Map;





/**
 * @author Matthias J. Sax
 */
class TestOrderedFileInputSpout extends AbstractOrderedFileInputSpout {
	private final static long serialVersionUID = -5336858069313450395L;
	
	@Override
	protected long extractTimestamp(String line) throws ParseException {
		return Long.parseLong(line.trim().split(",")[1].trim());
	}
	
	@Override
	public void activate() {}
	
	@Override
	public void deactivate() {}
	
	@Override
	public void ack(Object msgId) {}
	
	@Override
	public void fail(Object msgId) {}
	
	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
	
}
