/**
 * 
 */
package de.tuberlin.ise.benchfoundry.physicalschema.model.columnstore.schemagen;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import de.tuberlin.ise.benchfoundry.logicalschema.model.Attribute;
import de.tuberlin.ise.benchfoundry.logicalschema.model.Entity;
import de.tuberlin.ise.benchfoundry.logicalschema.model.RelationshipType;
import de.tuberlin.ise.benchfoundry.physicalschema.model.columnstore.CSColumn;
import de.tuberlin.ise.benchfoundry.physicalschema.model.columnstore.CSTable;
import de.tuberlin.ise.benchfoundry.util.BenchFoundryConfigData;

/**
 * merges two tables if possible
 * 
 * @author Dave
 *
 */
public class CSTableMerger {

	/**
	 * this is the maximum number of columns by which a {@link CSTable} may grow
	 * through a merge
	 */
	private int columnCountMergeThreshold = BenchFoundryConfigData._csPhysicalSchemaGenerationColumnCountMergeThreshold;

	/**
	 * There is also a constructor that sets the columnCountMergeThreshold
	 */
	public CSTableMerger() {
		super();
	}

	/**
	 * @param columnCountMergeThreshold
	 *            this is the maximum number of columns by which a
	 *            {@link CSTable} may grow through a merge
	 */
	public CSTableMerger(int columnCountMergeThreshold) {
		super();
		this.columnCountMergeThreshold = columnCountMergeThreshold;
	}

	/**
	 * merges the {@link CSTable}s one and two if possible
	 * 
	 * @param one
	 * @param two
	 * @return a merged version of one and two if merge was possible or null
	 *         otherwise
	 */
	public CSTable mergeTables(CSTable one, CSTable two) {
		boolean mergeable = fulfillsKeyCondition(one, two)
				&& fulfillsTableTypeCondition(one, two)
				&& fulfillsIndexCondition(one, two)
				&& fulfillsEntityRelationshipCondition(one, two)
				&& fulfillsColumnCountCondition(one, two)
				&& fulfillsRowCountCondition(one, two);
		if (mergeable)
			return executeMerge(one, two);
		else
			return null;
	}

	/**
	 * convenience method
	 * 
	 * @param tuple
	 *            must be size two or will throw a {@link RuntimeException}
	 * @return a merged version of both tables if merge was possible or null
	 *         otherwise
	 * @throws RuntimeException
	 *             if tuple has a size != 2
	 */
	public CSTable mergeTables(Set<CSTable> tuple) {
		if (tuple.size() != 2)
			throw new RuntimeException(
					"Size of parameter set \"tuple\" was not 2 as required.");
		CSTable one, two;
		Iterator<CSTable> it = tuple.iterator();
		one = it.next();
		two = it.next();
		return mergeTables(one, two);
	}

	/**
	 * asserts that a table merge would not result in two keys.
	 * 
	 * 
	 * @param one
	 *            first table
	 * @param two
	 *            second table
	 * @return whether the two tables' merge would meet the key condition.
	 *         Returns true if a merge is possible.
	 */
	private boolean fulfillsKeyCondition(CSTable one, CSTable two) {
		CSColumn a = one.getKeyColumn(), b = two.getKeyColumn();
		if (a != null && b != null)
			return a.equals(b);
		else
			return true;
	}

	/**
	 * asserts that only tables with the same table type are merged
	 * 
	 * @param one
	 *            first table
	 * @param two
	 *            second table
	 * @return true if both tables have the same table type, false otherwise.
	 */
	private boolean fulfillsTableTypeCondition(CSTable one, CSTable two) {
		return one.getTableType() == two.getTableType();
	}

	/**
	 * 
	 * checks whether the number of available index columns suffice to contain
	 * the index columns of both tables minus any index column that is row key
	 * in the respective other table.
	 * 
	 * @param one
	 *            first table
	 * @param two
	 *            second table
	 * @return whether the two tables' merge would meet the index condition.
	 *         Returns true if a merge is possible.
	 */
	private boolean fulfillsIndexCondition(CSTable one, CSTable two) {
		Set<CSColumn> set = new HashSet<CSColumn>();
		set.addAll(one.getIndexedColumns());
		set.addAll(two.getIndexedColumns());
		set.add(one.getKeyColumn());
		set.add(two.getKeyColumn());
		if (set.size() > CSTable.getMaxNoOfIndexColumns() + 1)
			return false;
		else
			return true;
	}

	/**
	 * Checks the following: Two tables can only be merged if their respectively
	 * contained entities are either a) subsets of each other, b) in 1:1
	 * relationships (there is at least one 1:1 relationship, the rest may be
	 * unrelated), or c) in 1:n or n:m relationship but for each of these
	 * relationships, the respective entities are already part of either one or
	 * two (we will not precompute any new joins).
	 * 
	 * 
	 * 
	 * @param one
	 *            first table
	 * @param two
	 *            second table
	 * @return whether the two tables' merge would meet the entity relationship.
	 *         condition. Returns true if a merge is possible.
	 */
	private boolean fulfillsEntityRelationshipCondition(CSTable one, CSTable two) {
		Set<Entity> entitiesOne = new HashSet<Entity>(), entitiesTwo = new HashSet<Entity>();
		for (CSColumn csc : one.getColumns())
			for (Attribute a : csc.getAttributes())
				entitiesOne.add(a.getEntity());
		for (CSColumn csc : two.getColumns())
			for (Attribute a : csc.getAttributes())
				entitiesOne.add(a.getEntity());
		// check relationships between tables
		if (entitiesOne.containsAll(entitiesTwo))
			return true;
		if (entitiesTwo.containsAll(entitiesOne))
			return true;
		// neither table is a subset of the other

		boolean noRelationship = true;
		for (Entity e1 : entitiesOne) {
			for (Entity e2 : entitiesTwo) {
				RelationshipType[] rel = e1.getRelationshipTo(e2);
				// check whether there is at least one relationship between the
				// two tables
				if (rel == null)
					continue;
				else
					noRelationship = false;
				// make sure that we do not precompute new joins
				if ((RelationshipType.isManyToMany(rel) || RelationshipType
						.isOneToMany(rel))
						&& !(entitiesOne.contains(e2) || entitiesTwo
								.contains(e1))) {
					return false;
				}
			}
		}
		if (noRelationship)
			return false;
		else
			return true;
	}

	/**
	 * checks whether either of the original table would grow during a merge by
	 * more than a threshold value in terms of the number of columns.
	 * 
	 * 
	 * @param one
	 *            first table
	 * @param two
	 *            second table
	 * @return whether the two tables' merge would meet the column count
	 *         condition. Returns true if a merge is possible.
	 */
	private boolean fulfillsColumnCountCondition(CSTable one, CSTable two) {
		Set<CSColumn> merged = new HashSet<CSColumn>(one.getColumns());
		merged.addAll(two.getColumns());
		if (merged.size() - one.getNumberOfColumns() > this.columnCountMergeThreshold
				|| merged.size() - two.getNumberOfColumns() > this.columnCountMergeThreshold)
			return false;
		else
			return true;
	}

	/**
	 * checks whether the amount of data stored would grow extensively through a
	 * merge, e.g., by adding additional columns to a table that was precomputed
	 * for a join on an n:m relationship
	 * 
	 * 
	 * @param one
	 *            first table
	 * @param two
	 *            second table
	 * @return whether the two tables' merge would meet the row count condition.
	 *         Returns true if a merge is possible.
	 */
	private boolean fulfillsRowCountCondition(CSTable one, CSTable two) {
		// FIXME this is hard to answer - at least not without additional
		// information. For now just return true.
		// maybe: return false whenever one table contains an n:m relationship?
		return true;
	}

	/**
	 * @param one
	 *            first table
	 * @param two
	 *            second table
	 * @return a merged version of one and two. Assert that all conditions are
	 *         met as the outcome of this method is, otherwise, undefined.
	 */
	private CSTable executeMerge(CSTable one, CSTable two) {
		CSTable newone = new CSTable();
		newone.addColumns(one.getColumns());
		newone.addColumns(two.getColumns());
		newone.addColumns(one.getIndexedColumns());
		newone.addColumns(two.getIndexedColumns());
		newone.addQueryIds(one.getQueryIds());
		newone.addQueryIds(two.getQueryIds());
		if (one.getKeyColumn() != null)
			newone.addColumn(one.getKeyColumn());
		else if (two.getKeyColumn() != null)
			newone.addColumn(two.getKeyColumn());
		return newone;
	}

}
