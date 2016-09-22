/**
 * 
 */
package de.tuberlin.ise.benchfoundry.physicalschema.model.columnstore;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import de.tuberlin.ise.benchfoundry.logicalschema.queries.LogicalQuery;
import de.tuberlin.ise.benchfoundry.physicalschema.model.AbstractPhysicalSchema;

/**
 * @author Dave
 *
 */
public class CSPhysicalSchema extends AbstractPhysicalSchema {

	/**
	 * all cstables that are part of this physical schema, key is some table
	 * name
	 */
	private final Map<String, CSTable> tables = new HashMap<String, CSTable>();

	/**holds a mapping from {@link LogicalQuery}.id to a sequence of requests*/
	private final Map<Integer, List<CSRequest>> requests = new HashMap<Integer, List<CSRequest>>();
	
	/**
	 * @return
	 * @see java.util.Map#size()
	 */
	public int getNumberOfTables() {
		return this.tables.size();
	}

	/**
	 * adds a {@link CSTable}
	 * 
	 * @param tablename
	 *            name of the table
	 * @param table
	 *            table which shall be added
	 * @return
	 * @see java.util.Map#put(java.lang.Object, java.lang.Object)
	 */
	public CSTable addTable(String tablename, CSTable table) {
		return this.tables.put(tablename, table);
	}

	/**
	 * adds a map of {@link CSTable}s
	 * 
	 * @param m
	 * @see java.util.Map#putAll(java.util.Map)
	 */
	public void putAll(Map<? extends String, ? extends CSTable> m) {
		this.tables.putAll(m);
	}

	/**
	 * 
	 * 
	 * @return the set of tablenames used
	 * @see java.util.Map#keySet()
	 */
	public Set<String> getTablenames() {
		return this.tables.keySet();
	}

	/**
	 * 
	 * 
	 * @return all tables
	 * @see java.util.Map#values()
	 */
	public Collection<CSTable> getTables() {
		return this.tables.values();
	}

	/**
	 * @return the tables
	 */
	public Map<String, CSTable> getTableMap() {
		return this.tables;
	}

	/**
	 * @param queryId
	 * @return
	 * @see java.util.Map#get(java.lang.Object)
	 */
	public List<CSRequest> getRequestsforQueryId(int queryId) {
		return this.requests.get(queryId);
	}

	/**
	 * @param queryId
	 * @param requests
	 * @return
	 * @see java.util.Map#put(java.lang.Object, java.lang.Object)
	 */
	public List<CSRequest> addRequestsForQueryId(int queryId, List<CSRequest> requests) {
		return this.requests.put(queryId, requests);
	}

	
	
}
