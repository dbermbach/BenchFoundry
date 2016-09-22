/**
 * 
 */
package de.tuberlin.ise.benchfoundry.logicalschema.queries;

import java.util.HashSet;
import java.util.Set;

import net.sf.jsqlparser.statement.Statement;
import de.tuberlin.ise.benchfoundry.logicalschema.model.Attribute;

/**
 * subclasses of this class represent safe database operations (SELECT, JOIN)
 * 
 * @author Dave
 *
 */
public abstract class ReadQuery extends LogicalQuery {

	/**
	 * @param query
	 * @param id
	 */
	public ReadQuery(String query, int id, Statement stmt) {
		super(query, id, stmt);
	}

	/** all attributes that are returned by this read query */
	private final Set<Attribute> resultSet = new HashSet<Attribute>();

	/**
	 * @return the resultSet
	 */
	public Set<Attribute> getResultSet() {
		return this.resultSet;
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.ise.benchfoundry.logicalschema.queries.LogicalQuery#getNonIndexedAffectedAttributes()
	 */
	@Override
	public Set<Attribute> getNonIndexedAffectedAttributes() {
		return getResultSet();
	}

}
