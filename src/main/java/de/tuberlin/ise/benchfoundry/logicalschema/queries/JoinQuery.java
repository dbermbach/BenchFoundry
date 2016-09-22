/**
 * 
 */
package de.tuberlin.ise.benchfoundry.logicalschema.queries;

import java.util.List;
import java.util.Set;

import net.sf.jsqlparser.statement.Statement;
import de.tuberlin.ise.benchfoundry.logicalschema.model.Attribute;

/**
 * represents a database operation that extracts information from more than one
 * logical entity
 * 
 * @author Dave
 *
 */
public class JoinQuery extends ReadQuery {

	/**
	 * @param query - the query string
	 * @param id - the identifier for the query
	 * @param stmt - the resulting parsed statement
	 */
	public JoinQuery(String query, int id, Statement stmt) {
		super(query, id, stmt);
	}

	/**
	 * @param query - the query string
	 * @param id - the identifier for the query
	 */
	public JoinQuery(String query, int id) {
		super(query, id, LogicalQuery.parseQuery(query));
	}

	/**
	 * 
	 * @return all join criteria of this query. the lists each contain two
	 *         entries that are the respective join criteria, e.g., customer.id
	 *         and order.cust_id
	 */
	public Set<? extends List<Attribute>> getJoinAttributes() {
		// FIXME
		return null;
	}
}
