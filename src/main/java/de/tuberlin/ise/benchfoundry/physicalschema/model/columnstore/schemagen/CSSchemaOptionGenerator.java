/**
 * 
 */
package de.tuberlin.ise.benchfoundry.physicalschema.model.columnstore.schemagen;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import de.tuberlin.ise.benchfoundry.logicalschema.model.Attribute;
import de.tuberlin.ise.benchfoundry.logicalschema.model.DataSchema;
import de.tuberlin.ise.benchfoundry.logicalschema.model.Entity;
import de.tuberlin.ise.benchfoundry.logicalschema.queries.InsertQuery;
import de.tuberlin.ise.benchfoundry.logicalschema.queries.JoinQuery;
import de.tuberlin.ise.benchfoundry.logicalschema.queries.LogicalQuery;
import de.tuberlin.ise.benchfoundry.logicalschema.queries.LogicalQueryRegistry;
import de.tuberlin.ise.benchfoundry.logicalschema.queries.ReadQuery;
import de.tuberlin.ise.benchfoundry.logicalschema.queries.SelectQuery;
import de.tuberlin.ise.benchfoundry.physicalschema.model.columnstore.CSColumn;
import de.tuberlin.ise.benchfoundry.physicalschema.model.columnstore.CSColumnType;
import de.tuberlin.ise.benchfoundry.physicalschema.model.columnstore.CSSimpleColumn;
import de.tuberlin.ise.benchfoundry.physicalschema.model.columnstore.CSTable;
import de.tuberlin.ise.benchfoundry.physicalschema.model.columnstore.CSTableType;
import de.tuberlin.ise.benchfoundry.util.SetOperationUtils;

/**
 * generates a set of schema options for column stores
 * 
 * 
 * @author Dave
 *
 */
public class CSSchemaOptionGenerator {

	/** holds the set of table variants (value) for each query ID (key) */
	private final Map<Integer, Set<CSTable>> tableVariants = new HashMap<Integer, Set<CSTable>>();

	/** holds all schema options (=cartesian product of all table variants) */
	private final Set<CSSchemaOption> schemaOptions = new HashSet<CSSchemaOption>();

	/**
	 * generates all possible schema options which could answer read queries
	 * with just a single read request.
	 */
	public void generateSchemaOptions() {
		createTableVariants(); // mapping options for individual queries
		createSchemaOptions(); // cartesian product of variants
		mergeTablesInOptions(); // e.g., when a table is a subset of another
		recreateInsertTables(); // for joins and inserts on the same entity
		createLookupTables(); // for update/delete queries
	}

	/**
	 * since a {@link CSSchemaOption} can comprise multiple occurrences of an
	 * attribute, update and delete queries need to affect all instances where
	 * this is the case. If the update/delete query requires an indexed
	 * attribute which is not supported by all tables where this
	 * to-be-deleted/updated attribute exists, then we have to add a lookup
	 * table that identifies the respective rows.
	 */
	private void createLookupTables() {
		// TODO Implement and document this method.
		throw new UnsupportedOperationException("Not implemented yet");
	}

	/**
	 * if there is an insert query for an entity A which is kept in a
	 * precomputed join table with entity B, then we need to add entity B as
	 * "INSERT_ORIGINAL" table to the schema option so that the join can be
	 * computed for the inserted instance of A. This method creates and adds
	 * these tables.
	 */
	private void recreateInsertTables() {
		Set<InsertQuery> inserts = new HashSet<InsertQuery>();
		inserts = LogicalQueryRegistry.getInstance().getQueriesByType(inserts,
				InsertQuery.class);
		if (inserts.size() == 0)
			return;
		for (CSSchemaOption option : schemaOptions) {
			List<CSTable> relevantTables = new ArrayList<CSTable>();
			List<InsertQuery> relevantQueries = new ArrayList<InsertQuery>();
			for (CSTable tbl : option.getTables()) {
				for (InsertQuery insert : inserts) {
					if (tbl.getContainedEntities().contains(
							insert.getTargetEntity())
							&& tbl.getContainedEntities().size() > 1) {
						relevantTables.add(tbl);
						relevantQueries.add(insert);
					}
				}
			}
			if (relevantTables.size() != 0) {
				Iterator<CSTable> itTbl = relevantTables.iterator();
				Iterator<InsertQuery> itQuery = relevantQueries.iterator();
				while (itTbl.hasNext() && itQuery.hasNext()) {
					option.addTables(buildOriginalInsertTables(itTbl.next(),
							itQuery.next()));
				}
			}
		}
	}

	/**
	 * @param tbl
	 *            a {@link CSTable} that contains some but not all entities of
	 *            the {@link InsertQuery} query
	 * @param query
	 *            an {@link InsertQuery}
	 * @return one or more tables which provide the necessary data to insert
	 *         rows into the join table tbl when running the insert query query
	 */
	private Collection<? extends CSTable> buildOriginalInsertTables(
			CSTable tbl, InsertQuery query) {
		// get all entities that are not an insert target
		Entity insertTarget = query.getTargetEntity();
		Set<String> tblEntities = tbl.getContainedEntities();
		tblEntities.remove(insertTarget);
		Set<CSTable> result = new HashSet<CSTable>();
		for (String entity : tblEntities) {
			// add a new insert original table each
			CSTable table = new CSTable();
			result.add(table);
			table.setTableType(CSTableType.INSERT_ORIGINAL);
			table.addQueryIds(query.getId());
			// add regular column
			for (Attribute attrib : DataSchema.getInstance()
					.getAttributesForEntity(entity)) {
				table.addColumn(CSColumn.createCSColumn(CSColumnType.REGULAR,
						attrib));
			}
			// determine key column (attribute that was used in the join)
			Set<Integer> queryIds = tbl.getQueryIds();
			Set<JoinQuery> joins = new HashSet<JoinQuery>();
			for (int i : queryIds) {
				LogicalQuery lq = LogicalQueryRegistry.getInstance()
						.getQueryForId(i);
				if (lq instanceof JoinQuery)
					joins.add((JoinQuery) lq);
			}
			boolean keyIsAdded = false;
			for (JoinQuery join : joins) {
				if (keyIsAdded)
					break;
				for (List<Attribute> joinAttribs : join.getJoinAttributes()) {
					if (joinAttribs.get(0).getEntity().getName().equals(entity)) {
						table.addColumn(CSColumn.createCSColumn(
								CSColumnType.KEY, joinAttribs.get(0)));
						keyIsAdded = true;
						break;
					} else if (joinAttribs.get(1).getEntity().getName()
							.equals(entity)) {
						table.addColumn(CSColumn.createCSColumn(
								CSColumnType.KEY, joinAttribs.get(1)));
						keyIsAdded = true;
						break;
					}
				}
			}
		}
		return result;
	}

	/**
	 * calculates for all schema options all potential ways in which tables
	 * could be merged into one and creates new options from each intermediate
	 * step. Stores all these options in field schemaOptions.
	 * 
	 */
	private void mergeTablesInOptions() {
		Set<CSSchemaOption> mergedOptions = new HashSet<CSSchemaOption>();
		for (CSSchemaOption option : schemaOptions) {
			mergedOptions.addAll(option.getMergedOptions());
		}
		schemaOptions.addAll(mergedOptions);
		System.out.println("Merged options, leading to a total number of "
				+ schemaOptions.size() + " schema options.");
	}

	/**
	 * calculates the cartesian product of all table variants and creates
	 * corresponding {@link CSSchemaOption} objects
	 * 
	 */
	private void createSchemaOptions() {
		List<List<CSTable>> variants = new ArrayList<List<CSTable>>();
		for (Set<CSTable> set : tableVariants.values()) {
			List<CSTable> tbls = new ArrayList<CSTable>();
			tbls.addAll(set);
			variants.add(tbls);
		}
		List<List<CSTable>> options = SetOperationUtils
				.getCartesianProduct(variants);
		for (List<CSTable> list : options) {
			CSSchemaOption option = new CSSchemaOption();
			option.addTables(list);
			schemaOptions.add(option);
		}
		System.out.println("Created a total number of " + schemaOptions.size()
				+ " schema options.");
	}

	/**
	 * creates for each query the set of possible representations within the
	 * column store
	 */
	private void createTableVariants() {
		System.out.println("Found "
				+ LogicalQueryRegistry.getInstance().getNumberOfQueries()
				+ " queries.");
		LogicalQueryRegistry registry = LogicalQueryRegistry.getInstance();
		Set<SelectQuery> selects = new HashSet<SelectQuery>();
		Set<JoinQuery> joins = new HashSet<JoinQuery>();
		Set<ReadQuery> reads = new HashSet<ReadQuery>();
		selects = registry.getQueriesByType(selects, SelectQuery.class);
		joins = registry.getQueriesByType(joins, JoinQuery.class);
		reads.addAll(selects);
		reads.addAll(joins);
		// for each select or join query, create a set of tables which contains
		// all table variants that could answer the respective query via just a
		// single get request to the column store
		int noOfIndexes = CSTable.getMaxNoOfIndexColumns();
		for (ReadQuery query : reads) {
			tableVariants.put(query.getId(),
					createTableVariantsForReadQuery(query, noOfIndexes));
		}
		// for the moment, log some statistics. replace with log4j later on
		int counter = 0;
		for (Set<CSTable> tbls : tableVariants.values())
			counter += tbls.size();
		System.out
				.println("Created a total of " + counter + " table variants.");
	}

	/**
	 * 
	 * @param query
	 *            a {@link ReadQuery} for which all table variants shall be
	 *            creates
	 * @param noOfIndexes
	 *            number of supported indexes in the {@link CSTable}
	 * @return a set of all possible representations this {@link ReadQuery}
	 *         could have in a column store so that the query can be answered
	 *         with just a single get request.
	 */
	private Set<CSTable> createTableVariantsForReadQuery(ReadQuery query,
			int noOfIndexes) {
		Set<CSTable> result = new HashSet<CSTable>();
		Set<Attribute> filters = query.getFilterAttributes(), returned = query
				.getNonIndexedAffectedAttributes();
		// add regular columns first and use deep copies later
		CSTable base = new CSTable();
		base.addQueryIds(query.getId());
		HashSet<Attribute> regularColumns = new HashSet<Attribute>(returned);
		regularColumns.remove(filters);
		for (Attribute a : regularColumns) {
			base.addColumn(new CSSimpleColumn(CSColumnType.REGULAR, a));
		}
		base.setTableType(CSTableType.REGULAR);

		// before looking at calculating all permutations deal with special
		// cases as
		// noOfIndexes==0 or filters.size()<2 is trivial to solve
		if (noOfIndexes == 0) {
			CSTable tbl = base.deepClone();
			result.add(tbl);
			CSColumn key = null;
			key = CSColumn.createCSColumn(CSColumnType.KEY, filters);
			if (key != null)
				tbl.addColumn(key);
			return result;
		}
		if (filters.size() == 0) {
			CSTable tbl = base.deepClone();
			result.add(tbl);
			return result;
		}
		if (filters.size() == 1 && noOfIndexes == 1) {
			CSTable filterAsKey = base.deepClone(), filterAsIndex = base
					.deepClone();
			CSColumn key = CSColumn.createCSColumn(CSColumnType.KEY, filters);
			CSColumn index = CSColumn.createCSColumn(CSColumnType.INDEX,
					filters);
			filterAsKey.addColumn(key);
			filterAsIndex.addColumn(index);
			result.add(filterAsIndex);
			result.add(filterAsKey);

			return result;
		} // all easy cases have been dealt with, now more than one index column
			// and more than one indexed attribute
		Set<Set<Attribute>> setOfSubsetsOfFilters = SetOperationUtils
				.getAllSubSets(filters.toArray(new Attribute[filters.size()]));
		// for each entry in this set use the entry as key and
		// {filters}\{entry} as index for a new table.
		for (Set<Attribute> keyset : setOfSubsetsOfFilters) {
			CSTable tbl = base.deepClone();
			Set<Attribute> indexes = new HashSet<Attribute>();
			indexes.addAll(filters);
			indexes.removeAll(keyset);
			// add key column if keyset is not the empty set
			if (keyset.size() > 0)
				tbl.addColumn(CSColumn.createCSColumn(CSColumnType.KEY, keyset));
			if (indexes.size() <= 1 || noOfIndexes == 1) {
				// add as index column
				CSColumn index = CSColumn.createCSColumn(CSColumnType.INDEX,
						indexes);
				if (index != null)
					tbl.addColumn(index);
				result.add(tbl);
				continue; // continue with next (key) subset
			}
			Set<CSTable> tables = addIndexColumnsToTable(tbl, indexes);
			result.addAll(tables);
		}

		return result;
	}

	/**
	 * @param tbl
	 *            base table without index columns
	 * @param indexes
	 *            filter attributes which should be distributed over index
	 *            columns
	 * @return a set of tables with all options of distributing filter
	 *         attributes across index columns based on the initial tbl
	 *         parameter, i.e., the only difference between the tables in the
	 *         result and tbl is the set of index columns.
	 */
	private Set<CSTable> addIndexColumnsToTable(CSTable tbl,
			Set<Attribute> indexes) {
		Set<CSTable> result = new HashSet<CSTable>();
		if (tbl.getNumberOfUnusedIndexColumns() == 1) {
			CSTable addme = tbl.deepClone();
			addme.addColumn(CSColumn
					.createCSColumn(CSColumnType.INDEX, indexes));
			result.add(addme);
			return result;
		}
		// still more than one open index column
		for (Set<Attribute> subset : SetOperationUtils.getAllSubSets(indexes
				.toArray(new Attribute[indexes.size()]))) {
			CSTable nextOption = tbl.deepClone();
			nextOption.addColumn(CSColumn.createCSColumn(CSColumnType.INDEX,
					subset));
			Set<Attribute> remaining = new HashSet<Attribute>(indexes);
			remaining.removeAll(subset);
			result.addAll(addIndexColumnsToTable(nextOption, remaining));
		}
		return result;
	}

	/**
	 * @return the tableVariants
	 */
	public Map<Integer, Set<CSTable>> getTableVariants() {
		return new TreeMap<Integer, Set<CSTable>>(this.tableVariants);
	}

	/**
	 * @return the schemaOptions
	 */
	public Set<CSSchemaOption> getSchemaOptions() {
		return new HashSet<CSSchemaOption>(this.schemaOptions);
	}

}
