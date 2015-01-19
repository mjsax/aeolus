/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package de.hub.cs.dbis.aeolus.batching;

import java.util.ArrayList;





/**
 * TODO
 * 
 * @author Matthias J. Sax
 */
public class BatchColumn extends ArrayList<Object> {
	private static final long serialVersionUID = -2215147192473477343L;
	
	/**
	 * Default constructor. Needed for deserialization.
	 */
	public BatchColumn() {
		super();
	}
	
	/**
	 * TODO
	 * 
	 * @param batchSize
	 */
	public BatchColumn(int batchSize) {
		super(batchSize);
	}
	
	/**
	 * TODO
	 */
	@Override
	public int hashCode() {
		assert (this.size() > 0);
		return this.get(0).hashCode();
	}
	
}
