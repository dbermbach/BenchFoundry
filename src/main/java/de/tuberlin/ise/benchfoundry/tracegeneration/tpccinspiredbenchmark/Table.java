package de.tuberlin.ise.benchfoundry.tracegeneration.tpccinspiredbenchmark;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Table {

	private static final Map<String, Table> instances = new HashMap<>();
	private final String name;
	private Column keyColumn;
	private final List<Column> foreignKeyColumns;
	private final List<Column> contentColumns;
	private long insertId;
	private long updateId;
	private long readId;

	private Table(String name) {
		this.name = name;
		// this.keyColumn = keyField;
		this.foreignKeyColumns = new ArrayList<>();
		this.contentColumns = new ArrayList<>();
	}

	private List<Column> allColumns() {
		List<Column> columns = new ArrayList<>();
		columns.add(keyColumn);
		columns.addAll(foreignKeyColumns);
		columns.addAll(contentColumns);
		return columns;
	}
	
	public long getInsertId() {
		return insertId;
	}

	public void setInsertId(long insertId) {
		this.insertId = insertId;
	}

	public long getUpdateId() {
		return updateId;
	}

	public void setUpdateId(long updateId) {
		this.updateId = updateId;
	}

	public long getReadId() {
		return readId;
	}

	public void setReadId(long readId) {
		this.readId = readId;
	}

	public String getDdl() {
		String ddl = "CREATE TABLE `" +name+ "` (" ;
		for (Column c : allColumns()) {
			if (foreignKeyColumns.contains(c))
				ddl += c.getName() +" " +c.getDomain().toDdl().replaceAll("UNIQUE", "")+ ", ";
			else
			ddl += c.getName() +" " +c.getDomain().toDdl()+ ", ";
		}
		ddl += "PRIMARY KEY(" +keyColumn.getName()+ "));";
		return ddl;
	}
	
	// INSERT into warehouse (w_id, w_name, w_street_1, w_street_2, w_city,
	// w_state, w_zip, w_tax, w_ytd) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
	public String getInsertOp() {
		String s = "INSERT into `" + name + "` (";
		if (keyColumn != null)
			s += keyColumn.getName() + ", ";
		for (int i = 0; i < foreignKeyColumns.size(); i++) {
			s += foreignKeyColumns.get(i).getName()+", ";
		}
		for (int i = 0; i < contentColumns.size(); i++) {
			s += contentColumns.get(i).getName()+ ", ";
		}
		s = s.substring(0, s.length()-2);
		s += ") VALUES (";
		int nFields = 0;
		if (keyColumn != null)
			nFields += 1;
		nFields += foreignKeyColumns.size() + contentColumns.size();
		for (int i = 0; i < nFields; i++) {
			s += "?, ";
		}
		s = s.substring(0, s.length()-2);
		s += ");";
		return s;
	}

	// Example: SELECT w_name, w_city FROM warehouse WHERE w_id=?;
	/**
	 * SQL SELECT Query for this table that retrieves a single row with provided
	 * local key and foreign keys.
	 * 
	 * @return
	 */
	public String getReadOp() {
		String op = "SELECT ";
		for (Column c : allColumns()) {
			op += c.getName() + ", ";
		}
		op = op.substring(0, op.length() - 2);
		op += " FROM `" + name + "` WHERE ";
		if (keyColumn != null)
			op += (keyColumn.getName() + "=? AND ");
		for (Column fCol : getForeignKeyColumns()) {
			op += (fCol.getName() + "=? AND ");
		}
		op = op.substring(0, op.length() - 5) + ";";
		return op;
	}

	/**
	 * Returns an SQL statement that updates all content fields of a single row
	 * for provided local key. Assumes in this first version that the table
	 * contains a local key that is the primary key for this table.
	 * 
	 * Example SQL UPDATE statement: UPDATE consbench SET myfield=? WHERE mykey=?;
	 * 
	 * @return SQL UPDATE statement
	 */
	public String getUpdateOp() {
		String op = "UPDATE `" + name + "` SET ";
		for (Column col : contentColumns) {
			op += col.getName() + "=?, ";
		}
		op = op.substring(0, op.length() - 2) + " WHERE ";
		if (keyColumn != null)
			op += (keyColumn.getName() + "=?;");
		return op;
	}

	/**
	 * Returns a list of parameters for a SQL update statement provided by
	 * <code>Table.getUpdateOp()</code>. Assumes in this first version that the
	 * table contains a local key that is the primary key for this table. All
	 * fields of content columns are updated. Fields representing foreign keys
	 * are NOT updated.
	 * 
	 * Example SQL UPDATE statement: UPDATE consbench SET myfield=? WHERE mykey=?;
	 * 
	 * @return Parameters for a SQL UPDATE statement
	 */
	public String[] getUpdateParams() {
		List<String> params = new ArrayList<>();
		if (keyColumn == null)
			throw new IllegalStateException("Table " +name+ " does not contain a column that represents a local key that is primary key of this table.");
		if(contentColumns.size() < 1)
			throw new IllegalStateException("Table " +name+ " only contains foreign keys and not local data fields. Update does not alter state of this row.");
		for (Column c : contentColumns) {
			params.add(c.getDomain().nextInsertField());
		}
		params.add(keyColumn.getDomain().nextReadField());
		return params.toArray(new String[params.size()]);
	}

	public String[] getInsertParams() {
		List<String> params = new ArrayList<>();
		if (keyColumn != null)
			params.add(keyColumn.getDomain().nextInsertField());
		for (Column c : foreignKeyColumns) {
			params.add(c.getDomain().nextReadField());
		}
		for (Column c : contentColumns) {
			params.add(c.getDomain().nextInsertField());
		}
		return params.toArray(new String[params.size()]);
	}
	
	public String insertParams() {
		return String.join("; ", getInsertParams());
	}
	
	public String updateParams() {
		return String.join("; ", getUpdateParams());
	}
	
	public String readParams() {
		return String.join("; ", getReadParams());
	}

	public String[] getReadParams() {
		List<String> params = new ArrayList<>();
		if (keyColumn != null)
			params.add(keyColumn.getDomain().nextReadField());
		for (Column c : foreignKeyColumns) {
			params.add(c.getDomain().nextReadField());
		}
		return params.toArray(new String[params.size()]);
	}

	public static Table c(String name) {
		Table t = new Table(name);
		instances.put(t.name, t);
		return t;
	}

	public static Table getInstance(String tableName) {
		if (!instances.containsKey(tableName))
			throw new IllegalArgumentException("No instane of class Table with name " + tableName + " exists.");
		return instances.get(tableName);
	}

	public String getName() {
		return name;
	}

	public Table kCol(Column keyColumn) {
		if (this.keyColumn != null && keyColumn != this.keyColumn)
			throw new IllegalArgumentException("The key column for table " + name + " is final.");
		this.keyColumn = keyColumn;
		return this;
	}

	public Table fCol(String tableName) {
		Table fTable = getInstance(tableName);
		Column fCol = fTable.getKeyColumn();
		// System.out.println("Adding foreign column " +fCol.getName()+ " from
		// table " +fTable.getName()+ " to table " +getName());
		foreignKeyColumns.add(fCol);
		return this;
	}

	public Table lCol(Column contentField) {
		contentColumns.add(contentField);
		return this;
	}

	public Column getKeyColumn() {
		return keyColumn;
	}

	public List<Column> getForeignKeyColumns() {
		return foreignKeyColumns;
	}

	public List<Column> getContentColumns() {
		return contentColumns;
	}

}
