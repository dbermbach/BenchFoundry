/**
 * 
 */
package de.tuberlin.ise.benchfoundry.logicalschema.queries;

import de.tuberlin.ise.benchfoundry.logicalschema.model.Attribute;
import de.tuberlin.ise.benchfoundry.logicalschema.model.Entity;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.StatementVisitorAdapter;
import net.sf.jsqlparser.statement.delete.Delete;

/**
 * represents a database operation that deletes one or more instances of an
 * entity
 * 
 * @author Dave
 *
 */
public class DeleteQuery extends WriteQuery {
	class LogicalDeleteStatementVisitor extends StatementVisitorAdapter {
		LogicalQuery _query;
		
		class LogicalExpressionVisitor extends ExpressionVisitorAdapter {
			LogicalQuery _query;
			LogicalExpressionVisitor(LogicalQuery query) {
				this._query = query;
			}
			@Override
			public void visit(Column column) {
				for (Entity entity: this._query.getAffectedEntities()) {
					Attribute attrib = entity.getAttributeByName(column.getColumnName());
					if (attrib != null) {
						this._query.getFilterAttributes().add(attrib);
						return;
					}
				}
				// TODO: throw an exception when a column is not found
			}
		}
		@Override
		public void visit(Delete delete) {
			Entity entity = this._query.getSchema().getEntityForName(delete.getTable().getName());
			this._query.getAffectedEntities().add(entity);
			delete.getWhere().accept(new LogicalExpressionVisitor(this._query));
		}

		public LogicalDeleteStatementVisitor(LogicalQuery query) {
			this._query = query;
		}
	}
	
	/**
	 * @param query
	 * @param id
	 * @param stmt 
	 */
	public DeleteQuery(String query, int id, Statement stmt) {
		super(query, id, stmt);
		stmt.accept(new LogicalDeleteStatementVisitor(this));
	}
}
