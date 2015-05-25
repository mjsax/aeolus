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
/*
 * Copyright 2015 Humboldt-Universität zu Berlin.
 *
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
 */
package storm.lrb.bolt;

import java.util.List;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.TypedQuery;
import storm.lrb.model.TollEntry;





/**
 * A {@link TollDataStore} using the Java Persistence API as storage backend. It is designed as singleton and you have
 * to retrieve the instance with {@link #getInstance() }.
 * 
 * Singleton implementation notes: - The instance is created at start, so no lazy loading because it doesn't make sense
 * - The constructor can be exposed in subclasses and thus the singleton status revoked
 * 
 * @author richter
 */
public class PersistenceTollDataStore implements TollDataStore {
	private final static EntityManagerFactory ENTITY_MANAGER_FACTORY;
	private final static EntityManager ENTITY_MANAGER;
	static {
		ENTITY_MANAGER_FACTORY = Persistence.createEntityManagerFactory("de.hub.cs.dbis.aeolus_lrb_jar_1.0-SNAPSHOTPU");
		ENTITY_MANAGER = ENTITY_MANAGER_FACTORY.createEntityManager();
	}
	private final static PersistenceTollDataStore INSTANCE = new PersistenceTollDataStore();
	
	public static PersistenceTollDataStore getInstance() {
		return INSTANCE;
	}
	
	protected PersistenceTollDataStore() {}
	
	@Override
	public Integer retrieveToll(int xWay, int day, int vehicleIdentifier) {
		TypedQuery<Integer> query = ENTITY_MANAGER.createQuery(
			"SELECT t.toll from TollEntry t WHERE t.vehicleIdentifier == ?1 AND t.aDay == ?2 AND t.xWay == ?3",
			Integer.class);
		query.setParameter(1, vehicleIdentifier);
		query.setParameter(2, day);
		query.setParameter(3, xWay);
		List<Integer> resultList = query.getResultList();
		if(resultList.isEmpty()) {
			return null;
		}
		return resultList.get(0);
	}
	
	@Override
	public void storeToll(int xWay, int day, int vehicleIdentifier, int toll) {
		TollEntry existingEntry = ENTITY_MANAGER.find(TollEntry.class, vehicleIdentifier);
		if(existingEntry == null) {
			existingEntry = new TollEntry(vehicleIdentifier, xWay, day, toll);
			ENTITY_MANAGER.persist(existingEntry);
		} else {
			existingEntry.setToll(toll);
			ENTITY_MANAGER.merge(existingEntry);
		}
	}
}
