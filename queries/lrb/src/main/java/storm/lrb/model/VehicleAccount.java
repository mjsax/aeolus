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

import java.io.Serializable;





/**
 * Object that keeps account information for a vehicle.
 */
public class VehicleAccount implements Serializable {
	
	private static final long serialVersionUID = 1L;
	private Integer vehicleIdentifier = 0;
	private int tollToday = 0;
	private Long tollTime = 0L;
	
	private final int day = 70;
	private Integer xWay;
	
	protected VehicleAccount() {}
	
	public VehicleAccount(Integer vehicleIdentifier, Integer xWay) {
		this.vehicleIdentifier = vehicleIdentifier;
		this.xWay = xWay;
		// TODO checken ob er an einem tag nur ein xway belegen kann, evtl auch rausnehmen
	}
	
	public VehicleAccount(int calculatedToll, PosReport pos) {
		if(pos.getTimer() == null) {
			throw new IllegalArgumentException("storm timer of the position timer is null");
		}
		this.vehicleIdentifier = pos.getVehicleIdentifier();
		this.xWay = pos.getSegmentIdentifier().getxWay();
		
		this.assessToll(calculatedToll, pos.getTimer().getOffset());
	}
	
	public VehicleAccount(AccountBalanceRequest bal) {
		this.vehicleIdentifier = bal.getVehicleIdentifier();
	}
	
	/**
	 * Adds newly assesed toll to the current account.
	 * 
	 * @param calculatedToll
	 *            amount
	 * @param time
	 *            of assesment
	 */
	@SuppressWarnings("FinalMethod")
	public final void assessToll(int calculatedToll, Long time) {
		
		this.tollToday += calculatedToll;
		this.tollTime = time;
		
	}
	
	public AccountBalance getAccBalanceNotification(AccountBalanceRequest accBalReq) {
		// TODO nach zweiter meinung fragen: Benchmarkspezifikation
		// widerspricht sich bei der Reihenfolge der Werte des Outputtuples.
		
		AccountBalance retValue = new AccountBalance(accBalReq.getTime(), accBalReq.getQueryIdentifier(),
			this.tollToday, this.tollTime, accBalReq.getCreated(), accBalReq.getTimer());
		return retValue;
	}
	
}
