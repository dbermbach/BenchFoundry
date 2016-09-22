/**
 * 
 */
package de.tuberlin.ise.benchfoundry.physicalschema.model.columnstore;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import de.tuberlin.ise.benchfoundry.logicalschema.model.Attribute;
import de.tuberlin.ise.benchfoundry.logicalschema.model.Entity;
import de.tuberlin.ise.benchfoundry.util.BenchFoundryConfigData;

/**
 * @author Dave
 *
 */
public class CSTable {

	/** columns within this table */
	private final SortedSet<CSColumn> columns = new TreeSet<CSColumn>();

	/** all indexed columns; subset of field columns */
	private final Set<CSColumn> indexedColumns = new TreeSet<CSColumn>();

	/** row key column; also contained in field columns */
	private CSColumn keyColumn;

	private final Set<Integer> queryIds = new TreeSet<Integer>();

	/** type of the table (regular, insert_original, lookup) */
	private CSTableType tableType;

	/** all entities from which we have used attributes in this table */
	private final Map<String, Integer> containedEntities = new TreeMap<String, Integer>();

	/**
	 * all attributes contained in this cstable, value is the number of
	 * occurences. Cached for efficiency.
	 */
	private final Map<Attribute, Integer> containedAttributes = new HashMap<Attribute, Integer>();

	/** defines the number of secondary index columns supported */
	private static int maxNoOfIndexColumns = BenchFoundryConfigData._csPhysicalSchemaMaxNoOfSecIndexColumns;

	/**
	 * adds a new {@link CSColumn} to this table to so that existing columns may
	 * be replaced according to the following rules:
	 * 
	 * <br>
	 * a) There can only be one key column. The 2nd key column replaces the
	 * first one. <br>
	 * b) The number of indexed columns is defined by maxNoOfIndexColumns. If
	 * this number would be exceeded, an existing one is replaced. <br>
	 * c) Columns with the same name are replaced, no matter what the type of
	 * the added column is.
	 * 
	 * @param column
	 *            column which shall be added
	 */
	public void addColumn(CSColumn column) {
		removeColumnWithName(column.getName());
		switch (column.type) {
		case KEY:
			if (keyColumn != null) {
				columns.remove(keyColumn);
			}
			keyColumn = column;
			break;
		case INDEX:
			if (indexedColumns.size() >= maxNoOfIndexColumns) {
				CSColumn removeMe = indexedColumns.iterator().next();
				indexedColumns.remove(removeMe);
				columns.remove(removeMe);
			}
			indexedColumns.add(column);
			break;
		case REGULAR:
			break;
		default:
			throw new RuntimeException(
					"Update switch/case - a new column type was obviously added.");
		}
		columns.add(column);
		addToContainedAttributesAndEntities(column);
	}

	/**
	 * convenience method, see addColumn(CSColumn)
	 * 
	 * @param columns
	 */
	public void addColumns(CSColumn... columns) {
		for (CSColumn c : columns)
			addColumn(c);
	}

	/**
	 * convenience method, see addColumn(CSColumn)
	 * 
	 * @param columns
	 */
	public void addColumns(Collection<? extends CSColumn> columns) {
		for (CSColumn c : columns)
			addColumn(c);
	}

	/**
	 * removes all columns with the specified name from this table
	 * 
	 * @param name
	 */
	private void removeColumnWithName(String name) {
		CSColumn existingOne = null;
		for (CSColumn c : columns)
			if (name.equals(c.getName())) {
				existingOne = c;
				break;
			}
		if (existingOne == null)
			return;
		columns.remove(existingOne);
		indexedColumns.remove(existingOne);
		if (existingOne == keyColumn)
			keyColumn = null;

		removeFromContainedEntities(existingOne);
		removeFromContainedAttributes(existingOne);

	}

	/**
	 * updates the fields containedEntities and containedAttributes
	 * 
	 * @param column
	 *            the new {@link CSColumn} which is added
	 */
	private void addToContainedAttributesAndEntities(CSColumn column) {
		Set<String> entities = new HashSet<String>();
		for (Attribute a : column.getAttributes()) {
			entities.add(a.getEntity().getName());
		}
		for (String entity : entities) {
			Integer entityOccurrence = containedEntities.get(entity);
			if (entityOccurrence == null)
				containedEntities.put(entity, 1);
			else
				containedEntities.put(entity, entityOccurrence + 1);
		}
		for (Attribute a : column.getAttributes()) {
			Integer attribOccurrence = containedAttributes.get(a);
			if (attribOccurrence == null)
				containedAttributes.put(a, 1);
			else
				containedAttributes.put(a, attribOccurrence + 1);
		}
	}

	/**
	 * updates the field containedAttributes
	 * 
	 * @param existingOne
	 *            column which shall be removed
	 */
	private void removeFromContainedAttributes(CSColumn existingOne) {
		for (Attribute a : existingOne.getAttributes()) {
			Integer number = containedAttributes.get(a);
			if (number != null) {
				number--;
				if (number > 0)
					containedAttributes.put(a, number);
				else
					containedAttributes.remove(a);
			}
		}

	}

	/**
	 * updates the field containedEntities
	 * 
	 * @param existingOne
	 *            column which shall be removed
	 */
	private void removeFromContainedEntities(CSColumn existingOne) {
		Set<String> entities = new HashSet<String>();
		for (Attribute a : existingOne.getAttributes()) {
			entities.add(a.getEntity().getName());
		}
		for (String entity : entities) {
			Integer entityOccurrence = containedEntities.get(entity);
			if (entityOccurrence == null)
				throw new RuntimeException(
						"There is a bug in the caching of stored entities per CSTable. Trying to remove entity "
								+ entity
								+ " from table "
								+ this
								+ " but cached entity list was: "
								+ containedEntities);
			else {
				entityOccurrence--;
				if (entityOccurrence <= 0)
					containedEntities.remove(entity);
				else
					containedEntities.put(entity, entityOccurrence);
			}
		}

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "CSTable " + this.columns + " with qIDs: " + queryIds;
	}

	/**
	 * 
	 * @return a short printable version of this object
	 */
	public String toSimpleString() {
		StringBuffer sb = new StringBuffer();
		sb.append("[");
		for (CSColumn csc : this.columns) {
			sb.append(csc.getSimpleName());
			switch (csc.type) {
			case KEY:
				sb.append("(K)");
				break;
			case INDEX:
				sb.append("(I)");
				break;
			case REGULAR:
				sb.append("(R)");
				break;
			}
			sb.append(',');
		}
		sb.deleteCharAt(sb.length() - 1);
		sb.append("]");
		sb.append(", queryIDs:");
		for (int i : queryIds)
			sb.append(i + ",");
		sb.deleteCharAt(sb.length() - 1);
		return sb.toString();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return toString().hashCode();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (obj == null)
			return false;
		if (!(obj instanceof CSTable))
			return false;
		return this.columns.equals(((CSTable) obj).columns);
	}

	/**
	 * @return the maxNoOfIndexColumns
	 */
	public static int getMaxNoOfIndexColumns() {
		return maxNoOfIndexColumns;
	}

	/**
	 * @param maxNoOfIndexColumns
	 *            the maxNoOfIndexColumns to set
	 */
	public static void setMaxNoOfIndexColumns(int maxNoOfIndexColumns) {
		CSTable.maxNoOfIndexColumns = maxNoOfIndexColumns;
	}

	/**
	 * @return the columns
	 */
	public SortedSet<CSColumn> getColumns() {
		TreeSet<CSColumn> copy = new TreeSet<CSColumn>();
		copy.addAll(columns);
		return copy;
	}

	/**
	 * @return the indexedColumns
	 */
	public SortedSet<CSColumn> getIndexedColumns() {
		TreeSet<CSColumn> copy = new TreeSet<CSColumn>();
		copy.addAll(indexedColumns);
		return copy;
	}

	/**
	 * 
	 * @return the number of additional index columns that could be added to
	 *         this table
	 */
	public int getNumberOfUnusedIndexColumns() {
		return maxNoOfIndexColumns - indexedColumns.size();
	}

	/**
	 * @return the keyColumn
	 */
	public CSColumn getKeyColumn() {
		return this.keyColumn;
	}

	/**
	 * 
	 * @return a deep copy of this {@link CSTable}
	 */
	public CSTable deepClone() {
		CSTable copy = new CSTable();
		for (CSColumn csc : columns)
			copy.addColumn(csc.deepClone());
		copy.queryIds.addAll(this.queryIds);
		copy.setTableType(this.tableType);
		return copy;
	}

	/**
	 * 
	 * @return the ids of all queries which can be answered by this table with a
	 *         single request
	 */
	public Set<Integer> getQueryIds() {
		return new TreeSet<Integer>(this.queryIds);
	}

	/**
	 * adds one or more queries to the set of queries which can be answered by
	 * this table with a single request
	 * 
	 * @param ids
	 *            one or more query ids
	 */
	public void addQueryIds(int... ids) {
		for (int id : ids)
			queryIds.add(id);
	}

	/**
	 * adds one or more queries to the set of queries which can be answered by
	 * this table with a single request
	 * 
	 * @param ids
	 *            a collection of query ids
	 */
	public void addQueryIds(Collection<Integer> ids) {
		queryIds.addAll(ids);
	}

	/**
	 * @return the total number of columns in this table (including key and
	 *         indexed columns)
	 * @see java.util.Set#size()
	 */
	public int getNumberOfColumns() {
		return this.columns.size();
	}

	/**
	 * @return the tableType
	 */
	public CSTableType getTableType() {
		return this.tableType;
	}

	/**
	 * @param tableType
	 *            the tableType to set
	 */
	public void setTableType(CSTableType tableType) {
		this.tableType = tableType;
	}

	/**
	 * 
	 * @param entity
	 * @return whether any attributes from the specified entity are stored in
	 *         this {@link CSTable}
	 */
	public boolean containsEntity(Entity entity) {
		return containsEntity(entity.getName());
	}

	/**
	 * 
	 * @param attrib
	 * @return whether the specified attribute is contained in any of the
	 *         columns of this {@link CSTable}
	 */
	public boolean containsAttribute(Attribute attrib) {
		return containedAttributes.containsKey(attrib);
	}

	/**
	 * 
	 * @param entity
	 * @return whether any attributes from the specified entity are stored in
	 *         this {@link CSTable}
	 */
	public boolean containsEntity(String entity) {
		return containedEntities.containsKey(entity);
	}

	/**
	 * 
	 * @return the names of all entities from which this table contains
	 *         attributes
	 */
	public Set<String> getContainedEntities() {
		return new HashSet<String>(containedEntities.keySet());
	}
}
