/**
 * 
 */
package de.tuberlin.ise.benchfoundry.logicalschema.queries;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.StatementVisitorAdapter;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.FromItemVisitorAdapter;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;
import net.sf.jsqlparser.statement.select.SelectItemVisitorAdapter;
import net.sf.jsqlparser.statement.select.SelectVisitorAdapter;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.statement.select.WithItem;
import de.tuberlin.ise.benchfoundry.logicalschema.model.Attribute;
import de.tuberlin.ise.benchfoundry.logicalschema.model.DataSchema;
import de.tuberlin.ise.benchfoundry.logicalschema.model.Entity;

/**
 * represents a database operation that extracts information from just one
 * logical entity
 * 
 * @author Dave
 *
 */
public class SelectQuery extends ReadQuery {
	class LogicalSelectStatementVisitor extends StatementVisitorAdapter {
		LogicalQuery _query;
		
		class LogicalSelectVisitorAdapter extends SelectVisitorAdapter {
			LogicalQuery _query;
			
			class LogicalSelectItemVisitor extends SelectItemVisitorAdapter {
				LogicalQuery _query;
				public LogicalSelectItemVisitor(LogicalQuery query) {
					this._query = query;
				}
				@Override
				public void visit(AllColumns columns) {
//					throw new UnsupportedOperationException("public void visit(AllColumns columns)");
				}
				@Override
				public void visit(AllTableColumns columns) {
//					throw new UnsupportedOperationException("public void visit(AllTableColumns columns)");
				}
				@Override
				public void visit(SelectExpressionItem item) {
					if (item.getExpression() instanceof Column) {
						Column column = ((Column)item.getExpression());
						this._query.getNonIndexedAffectedAttributes().add(new Attribute(this._query.getAffectedEntities().iterator().next(), column.getColumnName()));
					} else {
						// We ignore the other selected parameters for now
					}
 				}
			}
			
			class LogicalFromItemVisitor extends FromItemVisitorAdapter {
				LogicalQuery _query;
				LogicalFromItemVisitor(LogicalQuery query) {
					this._query = query;
				}
				@Override
				public void visit(Table table) {
					Entity entity = this._query.getSchema().getEntityForName(table.getName());
					this._query.getAffectedEntities().add(entity);
				}
			}

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

			public LogicalSelectVisitorAdapter(LogicalQuery query) {
				this._query = query;
			}
			
			@Override
			public void visit(PlainSelect plainSelect) {
				plainSelect.getFromItem().accept(new LogicalFromItemVisitor(this._query));
				
				SelectItemVisitor selectItemVisitor = new LogicalSelectItemVisitor(this._query);
				for (SelectItem item: plainSelect.getSelectItems()) {
					item.accept(selectItemVisitor);
				}

				Expression where = plainSelect.getWhere();
				if (where != null)
					plainSelect.getWhere().accept(new LogicalExpressionVisitor(this._query));
			}
			@Override
			public void visit(SetOperationList setOpList) {
				throw new UnsupportedOperationException("public void visit(SetOperationList setOpList)");
			}
			@Override
			public void visit(WithItem withItem) {
				throw new UnsupportedOperationException("public void visit(WithItem withItem)");
			}
		}
		
		public LogicalSelectStatementVisitor(LogicalQuery query) {
			this._query = query;
		}
		
	    @Override
	    public void visit(Select select) {
	    	select.getSelectBody().accept(new LogicalSelectVisitorAdapter(this._query));
	    }
	}
	/**
	 * @param query
	 * @param id
	 * @param stmt 
	 */
	public SelectQuery(String query, int id, Statement stmt) {
		super(query, id, stmt);
		stmt.accept(new LogicalSelectStatementVisitor(this));
	}

	/**
	 * @param string
	 * @param i
	 */
	public SelectQuery(String query, int id) {
		super(query, id, LogicalQuery.parseQuery(query));
	}

	/**
	 * 
	 * @return the entity from which this query retrieves attributes
	 */
	public Entity getAffectedEntity() {
		return getAffectedEntities().iterator().next();
	}

}
