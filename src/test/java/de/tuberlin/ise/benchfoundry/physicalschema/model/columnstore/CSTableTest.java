/**
 * 
 */
package de.tuberlin.ise.benchfoundry.physicalschema.model.columnstore;

import java.util.SortedSet;

import de.tuberlin.ise.benchfoundry.logicalschema.model.DataSchema;
import de.tuberlin.ise.benchfoundry.physicalschema.model.columnstore.CSColumn;
import de.tuberlin.ise.benchfoundry.physicalschema.model.columnstore.CSColumnType;
import de.tuberlin.ise.benchfoundry.physicalschema.model.columnstore.CSCompoundColumn;
import de.tuberlin.ise.benchfoundry.physicalschema.model.columnstore.CSSimpleColumn;
import de.tuberlin.ise.benchfoundry.physicalschema.model.columnstore.CSTable;
import junit.framework.TestCase;

/**
 * @author Dave
 *
 */
public class CSTableTest extends TestCase {

	static CSColumn simplekey, simpleindex, simplereg, compindex, compreg,
			compkey;
	
	private final String CUSTOMER = "CSTT_customer";
	private final String FNAME = "fname";
	private final String LNAME = "lname";
	private final String ID = "id";
	private final String ADDRESS = "address";
	/*
	 * (non-Javadoc)
	 * 
	 * @see junit.framework.TestCase#setUp()
	 */
	protected void setUp() throws Exception {
		super.setUp();
		DataSchema schema = DataSchema.getInstance();
		schema.createNewEntity(CUSTOMER, FNAME, LNAME, ID, ADDRESS);
		schema.setReadyForUse();
		simplekey = new CSSimpleColumn(CSColumnType.KEY,
				schema.getAttributeByName(ID, CUSTOMER));
		simpleindex = new CSSimpleColumn(CSColumnType.INDEX,
				schema.getAttributeByName(FNAME, CUSTOMER));
		simplereg = new CSSimpleColumn(CSColumnType.REGULAR,
				schema.getAttributeByName(ADDRESS, CUSTOMER));
		compkey = new CSCompoundColumn(CSColumnType.KEY,
				schema.getAttributeByName(FNAME, CUSTOMER),
				schema.getAttributeByName(ID, CUSTOMER));
		compindex = new CSCompoundColumn(CSColumnType.INDEX,
				schema.getAttributeByName(ID, CUSTOMER),
				schema.getAttributeByName(LNAME, CUSTOMER));
		compreg = new CSCompoundColumn(CSColumnType.REGULAR,
				schema.getAttributeByName(FNAME, CUSTOMER),
				schema.getAttributeByName(LNAME, CUSTOMER));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see junit.framework.TestCase#tearDown()
	 */
	protected void tearDown() throws Exception {
		super.tearDown();
	}

	/**
	 * Test method for
	 * {@link de.tuberlin.ise.benchfoundry.physicalschema.model.columnstore.CSTable#addColumns(de.tuberlin.ise.benchfoundry.physicalschema.model.columnstore.CSColumn[])}
	 * .
	 */
	public void testAddColumnCSColumnArray() {
		CSTable tbl = new CSTable();
		tbl.addColumn(compindex);
		tbl.addColumn(simplekey);
		tbl.addColumn(simplereg);
		// check that the key is replaced
		tbl.addColumn(compkey);
		assertSame(compkey, tbl.getKeyColumn());
		// check that the index column is replaced
		tbl.addColumn(simpleindex);
		assertSame(simpleindex, tbl.getIndexedColumns().iterator().next());
		// check that the same name replaces both existing columns
		DataSchema schema = DataSchema.getInstance();
		CSColumn newindex = new CSSimpleColumn(CSColumnType.REGULAR,
				schema.getAttributeByName(FNAME, CUSTOMER));
		CSColumn newreg = new CSSimpleColumn(CSColumnType.KEY,
				schema.getAttributeByName(ADDRESS, CUSTOMER));
		CSColumn newkey = new CSCompoundColumn(CSColumnType.INDEX,
				schema.getAttributeByName(FNAME, CUSTOMER),
				schema.getAttributeByName(ID, CUSTOMER));
		tbl.addColumn(newindex);
		tbl.addColumn(newreg);
		tbl.addColumn(newkey);
		assertTrue(tbl.getColumns().size() == 3
				&& tbl.getColumns().contains(newindex)
				&& tbl.getColumns().contains(newreg)
				&& tbl.getColumns().contains(newkey));
	}

	/**
	 * Test method for
	 * {@link de.tuberlin.ise.benchfoundry.physicalschema.model.columnstore.CSTable#getColumns()
	 */
	public void testGetColumns() {
		CSTable tbl = new CSTable();
		tbl.addColumn(compindex);
		tbl.addColumn(simplekey);
		tbl.addColumn(simplereg);
		SortedSet<CSColumn> set = tbl.getColumns();
		set.remove(simplereg);
		set.add(compreg);
		assertTrue(!tbl.getColumns().equals(set));
	}

	/**
	 * Test method for
	 * {@link de.tuberlin.ise.benchfoundry.physicalschema.model.columnstore.CSTable#getIndexedColumns()
	 */
	public void testGetIndexedColumns() {
		CSTable tbl = new CSTable();
		tbl.addColumn(compindex);
		tbl.addColumn(simplekey);
		tbl.addColumn(simplereg);
		SortedSet<CSColumn> set = tbl.getIndexedColumns();
		set.clear();
		set.add(compreg);
		assertTrue(!tbl.getIndexedColumns().equals(set));
	}

	/**
	 * Test method for
	 * {@link de.tuberlin.ise.benchfoundry.physicalschema.model.columnstore.CSTable#equals()
	 */
	public void testEquals() {
		CSTable tbl = new CSTable();
		tbl.addColumn(compindex);
		tbl.addColumn(simplekey);
		CSTable identical = new CSTable();
		identical.addColumn(compindex);
		identical.addColumn(simplekey);
		assertTrue(tbl.equals(identical));
		// other columns
		CSTable diff = new CSTable();
		assertTrue(!tbl.equals(diff));
		diff.addColumn(compindex);
		assertTrue(!tbl.equals(diff));
		// compare to null or other classes
		assertTrue(!tbl.equals(null));
		assertTrue(!tbl.equals("test"));
	}

	/**
	 * Test method for
	 * {@link de.tuberlin.ise.benchfoundry.physicalschema.model.columnstore.CSTable#hashcode()}
	 * .
	 */
	public void testHashcode() {
		CSTable tbl = new CSTable();
		tbl.addColumn(compindex);
		tbl.addColumn(simplekey);
		CSTable identical = new CSTable();
		identical.addColumn(compindex);
		identical.addColumn(simplekey);
		assertTrue(tbl.hashCode() == identical.hashCode());
		// other columns
		CSTable diff = new CSTable();
		assertTrue(tbl.hashCode() != diff.hashCode());
		diff.addColumn(compindex);
		assertTrue(tbl.hashCode() != diff.hashCode());
	}
}
