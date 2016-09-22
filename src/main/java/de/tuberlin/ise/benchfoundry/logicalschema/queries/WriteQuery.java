/**
 * 
 */
package de.tuberlin.ise.benchfoundry.logicalschema.queries;

import net.sf.jsqlparser.statement.Statement;

/**
 * subclasses of this class represent operations that alter the state of the database (INSERT, UPDATE, DELETE)
 * 
 * @author Dave
 *
 */
public abstract class WriteQuery extends LogicalQuery {

	/**
	 * @param query
	 * @param id
	 */
	public WriteQuery(String query, int id, Statement stmt) {
		super(query, id, stmt);
	}

}
