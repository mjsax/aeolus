package de.hub.cs.dbis.lrb.operators;

import backtype.storm.tuple.Tuple;
import de.hub.cs.dbis.aeolus.queries.utils.AbstractFileOutputBolt;





/**
 * @author Matthias J. Sax
 */
public class SpoutDataFileOutputBolt extends AbstractFileOutputBolt {
	private static final long serialVersionUID = 412459844575730202L;
	
	@Override
	protected String tupleToString(Tuple t) {
		return t.getLong(0) + "," + t.getString(1) + "\n";
	}
	
}
