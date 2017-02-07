/**
 * 
 */
package de.tuberlin.ise.benchfoundry.scheduling;

import de.tuberlin.ise.benchfoundry.logicalschema.queries.LogicalQuery;

/**
 * represents the combination of a single {@link LogicalQuery} and the
 * corresponding parameter sets for its actual execution.
 * 
 * @author Dave
 *
 */
public class BusinessOperation {

	/** the ID of this business operation */
	private final long id;

	/** the ID of the underlying {@link LogicalQuery} which shall be executed */
	private final int logicalQueryId;

	/**
	 * the ID of the param set which shall be used in combination with the
	 * specified {@link LogicalQuery}
	 */
	private final int paramsetId;

	/**
	 * the ID of the custom param set which shall be used in combination with
	 * the specified {@link LogicalQuery}, custom parameters may or may not be
	 * interpreted by concrete database connectors.
	 */
	private final int custParamsetId;

	/**
	 * @param id
	 *            the ID of this business operation
	 * @param logicalQueryId
	 *            the ID of the underlying {@link LogicalQuery} which shall be
	 *            executed
	 * @param paramsetId
	 *            the ID of the param set which shall be used in combination
	 *            with the specified {@link LogicalQuery}
	 * @param custParamsetId
	 *            the ID of the custom param set which shall be used in
	 *            combination with the specified {@link LogicalQuery}, custom
	 *            parameters may or may not be interpreted by concrete database
	 *            connectors.
	 */
	BusinessOperation(long id, int logicalQueryId, int paramsetId,
			int custParamsetId) {
		super();
		this.id = id;
		this.logicalQueryId = logicalQueryId;
		this.paramsetId = paramsetId;
		this.custParamsetId = custParamsetId;
	}

	
	/**
	 * @return the id
	 */
	public long getId() {
		return this.id;
	}

	/**
	 * @return the logicalQueryId
	 */
	public int getLogicalQueryId() {
		return this.logicalQueryId;
	}

	/**
	 * @return the paramsetId
	 */
	public int getParamsetId() {
		return this.paramsetId;
	}

	/**
	 * @return the custParamsetId
	 */
	public int getCustParamsetId() {
		return this.custParamsetId;
	}

	/**
	 * Returns a string representation of the business operation suitable for tracing and logging.
	 */
	public String toString() {
		return this.logicalQueryId + ";" + this.paramsetId + ";" + this.custParamsetId;
	}
}
