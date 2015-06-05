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

/**
 * Immutable version of the Accident object for serialization.
 */
public class AccidentImmutable extends Accident {
	private static final long serialVersionUID = 1L;

	protected AccidentImmutable() {
		super();
	}

	public AccidentImmutable(Accident accident) {
		super(accident);
	}

	public AccidentImmutable(PosReport report) {
		super(report);
	}

	public AccidentImmutable(long startMinute, long lastUpdateTime, int position, int maxPos, int minPos,
		PosReport posReport) {
		super(startMinute, lastUpdateTime, position, maxPos, minPos, posReport);
	}

	@Override
	public void setOver(boolean over) {
		throw new UnsupportedOperationException(String.format("instances of %s are immutable", AccidentImmutable.class));
	}

	@Override
	public void setLastUpdateTime(long lastUpdateTime) {
		throw new UnsupportedOperationException(String.format("instances of %s are immutable", AccidentImmutable.class));
	}

	@Override
	public void setPosReport(PosReport posReport) {
		throw new UnsupportedOperationException(String.format("instances of %s are immutable", AccidentImmutable.class));
	}

}
