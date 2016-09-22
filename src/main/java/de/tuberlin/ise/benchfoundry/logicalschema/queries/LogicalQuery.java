/**
 * 
 */
package de.tuberlin.ise.benchfoundry.logicalschema.queries;

import java.util.HashSet;
import java.util.Set;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.statement.delete.Delete;
import de.tuberlin.ise.benchfoundry.logicalschema.model.Attribute;
import de.tuberlin.ise.benchfoundry.logicalschema.model.DataSchema;
import de.tuberlin.ise.benchfoundry.logicalschema.model.Entity;
import de.tuberlin.ise.benchfoundry.physicalschema.model.AbstractRequest;

/**
 * describes a query against the logical data model. Each query is later
 * translated into a sequence of requests against a concrete datastore (list of
 * {@link AbstractRequest}.
 * 
 * @author Dave
 *
 */
public abstract class LogicalQuery {
	/**
	 * Reference to the DataSchema.
	 */
	private final DataSchema schema = DataSchema.getInstance();
	
	/**
	 * all entities that are affected by this query (entities that are
	 * written/read)
	 */
	private final Set<Entity> affectedEntities = new HashSet<Entity>();

	/** all attributes which should be indexed */
	private final Set<Attribute> filterAttributes = new HashSet<Attribute>();

	/**
	 * all attributes which are either returned (read queries) or
	 * updated/deleted/inserted
	 */
	private final Set<Attribute> affectedAttributes = new HashSet<Attribute>();

	/** unique id of this query */
	private final int id;

	/** the JSQLParser Statement generated from the input query */
	private Statement stmt;

	/** the original sql string used as input */
	private String sqlString;

	/**
	 * @param query
	 * @param id2
	 * @return
	 */
	public static Statement parseQuery(String query) {
		Statement stmt = null;
		try {
			stmt = CCJSqlParserUtil.parse(query);
		} catch (JSQLParserException e) {
			e.printStackTrace();
		}
		return stmt;
	}

	public static LogicalQuery makeQuery(String query, int id) throws UnknownQueryTypeException {
		Statement stmt = parseQuery(query);
		if (stmt instanceof Select) {
			return new SelectQuery(query, id, stmt);
		} else if (stmt instanceof Insert) {
			return new InsertQuery(query, id, stmt);
		} else if (stmt instanceof Update) {
			return new UpdateQuery(query, id, stmt);
		} else if (stmt instanceof Delete) {
			return new DeleteQuery(query, id, stmt);
		}
		throw new UnknownQueryTypeException("query");
	}

	/**
	 * 
	 * @param query
	 *            SQL string
	 * @param id
	 *            unique id which is used internally to identify this query
	 */
	public LogicalQuery(String query, int id, Statement stmt) {
		this.id = id;
		this.sqlString = query;
		this.stmt = stmt;
	}

	/**
	 * @return the affectedEntities
	 */
	public Set<Entity> getAffectedEntities() {
		return this.affectedEntities;
	}

	/**
	 * 
	 * @return all attributes that should be indexed
	 */
	public Set<Attribute> getFilterAttributes() {
		return new HashSet<Attribute>(filterAttributes);
	}

	/**
	 * 
	 * @return all attributes that are part of the result of this query
	 */
	public Set<Attribute> getNonIndexedAffectedAttributes() {
		return new HashSet<Attribute>(affectedAttributes);
	}

	/**
	 * @return the id
	 */
	public int getId() {
		return this.id;
	}

	/**
	 * @return a reference to the schema repository
	 */
	public DataSchema getSchema() {
		return schema;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return this.sqlString;
	}


}
