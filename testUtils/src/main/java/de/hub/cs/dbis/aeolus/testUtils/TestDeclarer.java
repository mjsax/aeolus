package de.hub.cs.dbis.aeolus.testUtils;

import java.util.ArrayList;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;





/**
 * @author Matthias J. Sax
 */
public class TestDeclarer implements OutputFieldsDeclarer {
	public ArrayList<Fields> schema = new ArrayList<Fields>();
	public ArrayList<String> streamId = new ArrayList<String>();
	public ArrayList<Boolean> direct = new ArrayList<Boolean>();
	
	@Override
	public void declare(Fields fields) {
		this.schema.add(fields);
		this.streamId.add(null);
		this.direct.add(false);
	}
	
	@Override
	public void declare(boolean direct, Fields fields) {
		this.schema.add(fields);
		this.streamId.add(null);
		this.direct.add(direct);
	}
	
	@Override
	public void declareStream(String streamId, Fields fields) {
		this.schema.add(fields);
		this.streamId.add(streamId);
		this.direct.add(false);
	}
	
	@Override
	public void declareStream(String streamId, boolean direct, Fields fields) {
		this.schema.add(fields);
		this.streamId.add(streamId);
		this.direct.add(direct);
	}
	
}
