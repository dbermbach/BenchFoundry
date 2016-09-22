/**
 * 
 */
package de.tuberlin.ise.benchfoundry.logicalschema.queries;

import de.tuberlin.ise.benchfoundry.logicalschema.model.Attribute;
import de.tuberlin.ise.benchfoundry.logicalschema.model.Entity;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.StatementVisitorAdapter;
import net.sf.jsqlparser.statement.update.Update;

/**
 * represents a database operation that updates one or more attributes for
 * entities of just one type
 * 
 * @author Dave
 *
 */
public class UpdateQuery extends WriteQuery {
	class LogicalUpdateStatementVisitor extends StatementVisitorAdapter {
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
		public void visit(Update update) {
			for (Table table: update.getTables()) {
				Entity entity = this._query.getSchema().getEntityForName(table.getName());
				this._query.getAffectedEntities().add(entity);
			}
			update.getWhere().accept(new LogicalExpressionVisitor(this._query));
		}

		public LogicalUpdateStatementVisitor(LogicalQuery query) {
			this._query = query;
		}
	}

	/**
	 * @param query
	 * @param id
	 * @param stmt 
	 */
	public UpdateQuery(String query, int id, Statement stmt) {
		super(query, id, stmt);
		stmt.accept(new LogicalUpdateStatementVisitor(this));
	}

}
