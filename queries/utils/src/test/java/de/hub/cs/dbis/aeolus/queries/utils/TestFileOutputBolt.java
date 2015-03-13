package de.hub.cs.dbis.aeolus.queries.utils;

import backtype.storm.tuple.Tuple;





/**
 * @author Matthias J. Sax
 */
public class TestFileOutputBolt extends AbstractFileOutputBolt {
	private static final long serialVersionUID = -956984089329568377L;
	
	@Override
	protected String tupleToString(Tuple t) {
		return t.toString();
	}
	
}
