package de.hub.cs.dbis.lrb.datatypes;

/**
 * The base class for all LRB output tuples (including intermediate result tuples).<br/>
 * <br/>
 * All output tuples do have the following attributes: TYPE, TIME, EMIT
 * <ul>
 * <li>TYPE: the tuple type ID</li>
 * <li>TIME: 'the timestamp of the input tuple that triggered the tuple to be generated' (in milliseconds)</li>
 * <li>EMIT: 'the timestamp immediately prior to emitting the tuple' (in milliseconds)</li>
 * </ul>
 * 
 * @author mjsax
 */
public abstract class AbstractOutputTuple extends AbstractLRBTuple {
	private static final long serialVersionUID = 6525749728643815820L;
	
	// attribute indexes
	/** The index of the EMIT attribute. */
	public final static int EMIT_IDX = 2;
	
	
	
	protected AbstractOutputTuple() {
		super();
	}
	
	protected AbstractOutputTuple(Short type, Long time, Long emit) {
		super(type, time);
		
		assert (emit != null);
		assert (emit.longValue() >= time.longValue());
		super.add(EMIT_IDX, emit);
		
		assert (super.size() == 3);
	}
	
	
	
	/**
	 * Returns the emit timestamp (in milliseconds) of this {@link AbstractOutputTuple}.
	 * 
	 * @return the emit timestamp of this tuple
	 */
	public final Long getEmit() {
		return (Long)super.get(EMIT_IDX);
	}
	
}
