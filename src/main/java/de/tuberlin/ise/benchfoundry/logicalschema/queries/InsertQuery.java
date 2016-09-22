/**
 * 
 */
package de.tuberlin.ise.benchfoundry.logicalschema.queries;

import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.StatementVisitorAdapter;
import net.sf.jsqlparser.statement.insert.Insert;
import de.tuberlin.ise.benchfoundry.logicalschema.model.Entity;

/**
 * represents a database operation that creates a new instance of an entity in a
 * database
 * 
 * @author Dave
 *
 */
public class InsertQuery extends WriteQuery {
		class LogicalInsertStatementVisitor extends StatementVisitorAdapter {
		LogicalQuery _query;
		
		@Override
		public void visit(Insert insert) {
			Entity entity = this._query.getSchema().getEntityForName(insert.getTable().getName());
			this._query.getAffectedEntities().add(entity);
		}

		public LogicalInsertStatementVisitor(LogicalQuery query) {
			this._query = query;
		}
	}

	/**
	 * @param query
	 * @param id
	 * @param stmt 
	 */
	public InsertQuery(String query, int id, Statement stmt) {
		super(query, id, stmt);
		stmt.accept(new LogicalInsertStatementVisitor(this));
	}

	/**
	 * 
	 * @return the entity for which a new instance shall be inserted
	 */
	public Entity getTargetEntity() {
		return getAffectedEntities().iterator().next();
	}
}
