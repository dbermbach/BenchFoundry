/**
 * 
 */
package de.tuberlin.ise.benchfoundry.physicalschema.model.columnstore.schemagen;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import de.tuberlin.ise.benchfoundry.physicalschema.model.columnstore.CSTable;
import de.tuberlin.ise.benchfoundry.util.SetOperationUtils;

/**
 * holds
 * 
 * @author Dave
 *
 */
public class CSSchemaOption {

	/** holds all tables of this option */
	private final Set<CSTable> tables = new HashSet<CSTable>();

	/**
	 * @return
	 * @see java.util.Set#size()
	 */
	public int getNumberOfTables() {
		return this.tables.size();
	}

	/**
	 * @return
	 * @see java.util.Set#iterator()
	 */
	public Iterator<CSTable> iterator() {
		return this.tables.iterator();
	}

	/**
	 * @param e
	 * @return
	 * @see java.util.Set#add(java.lang.Object)
	 */
	public boolean addTable(CSTable e) {
		return this.tables.add(e);
	}

	/**
	 * @param c
	 * @return
	 * @see java.util.Set#addAll(java.util.Collection)
	 */
	public boolean addTables(Collection<? extends CSTable> c) {
		return this.tables.addAll(c);
	}

	/**
	 * 
	 * @return all tables that are part of this {@link CSSchemaOption}
	 */
	public Set<CSTable> getTables() {
		return new HashSet<CSTable>(tables);
	}

	/**
	 * 
	 * @return a deep copy of this instance so that changes to the returned
	 *         value will not affect this instance.
	 */
	public CSSchemaOption deepClone() {
		CSSchemaOption clone = new CSSchemaOption();
		for (CSTable cst : getTables())
			clone.addTable(cst.deepClone());
		return clone;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return tables.hashCode();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		return tables.equals(obj);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer("CSSchemaOption:");
		for (CSTable tbl : tables)
			sb.append("\n\t" + tbl.toSimpleString());
		return sb.toString();
	}

	/**
	 * builds a tree of {@link CSSchemaOption}s with options, where no further
	 * tables can be merged, as leaves. Adds all these options including all
	 * intermediate steps to a result set.
	 * 
	 * @return this set.
	 */
	public Set<CSSchemaOption> getMergedOptions() {
		Set<CSSchemaOption> mergedOptions = new HashSet<CSSchemaOption>();
		CSTableMerger merger = new CSTableMerger();
		for (Set<CSTable> tuple : SetOperationUtils.getAllSizeTwoSubsets(this
				.getTables())) {
			if (tuple.size() == 1)
				continue;
			CSTable mergedTable = merger.mergeTables(tuple);
			if (mergedTable != null) {
				CSSchemaOption newOne = this.deepClone();
				newOne.tables.removeAll(tuple);
				newOne.addTable(mergedTable);
				mergedOptions.add(newOne);
				mergedOptions.addAll(newOne.getMergedOptions());
			}
		}

		return mergedOptions;

	}

}
