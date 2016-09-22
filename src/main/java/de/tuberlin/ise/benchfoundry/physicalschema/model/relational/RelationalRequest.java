/**
 * 
 */
package de.tuberlin.ise.benchfoundry.physicalschema.model.relational;

import de.tuberlin.ise.benchfoundry.physicalschema.model.AbstractRequest;

/**
 * @author Dave
 *
 */
public class RelationalRequest extends AbstractRequest {

	private final String sqlQuery;

	/**
	 * @param sqlQuery
	 */
	public RelationalRequest(String sqlQuery, int logicalQueryId) {
		super(logicalQueryId);
		this.sqlQuery = sqlQuery;
	}

	/**
	 * @return the sqlQuery
	 */
	public String getSqlQuery() {
		return this.sqlQuery;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "RelationalRequest [queryId=" + getLogicalQueryId()
				+ ", sqlQuery=" + this.sqlQuery + "]";
	}

}
