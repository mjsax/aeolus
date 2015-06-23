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
package storm.lrb.model;

import de.hub.cs.dbis.lrb.datatypes.AbstractInputTuple;
import de.hub.cs.dbis.lrb.datatypes.AbstractLRBTuple;





/**
 * Object representing account balance requests. Balance requests have the form (Type = 2, Time, VID, QID) where QID is
 * a query identifier.
 * 
 */
public class AccountBalanceRequest extends AbstractInputTuple {
	
	private static final long serialVersionUID = 1L;
	/**
	 * QID is an integer query identiﬁer
	 */
	private Integer queryIdentifier;
	
	
	protected AccountBalanceRequest() {
		super();
		
	}
	
	public AccountBalanceRequest(Long time, Integer vehicleIdentifier, int queryIdentifier) {
		super(AbstractLRBTuple.ACCOUNT_BALANCE_REQUEST, time, vehicleIdentifier);
		this.queryIdentifier = queryIdentifier;
	}
	
	
	public Integer getQueryIdentifier() {
		return this.queryIdentifier;
	}
	
}
