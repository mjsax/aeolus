package de.hub.cs.dbis.aeolus.queries.utils;

import java.text.ParseException;
import java.util.Map;





/**
 * @author Matthias J. Sax
 */
class TestOrderedFileInputSpout extends AbstractOrderedFileInputSpout {
	private static final long serialVersionUID = -5336858069313450395L;
	
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
