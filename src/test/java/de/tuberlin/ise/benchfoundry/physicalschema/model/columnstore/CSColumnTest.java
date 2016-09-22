/**
 * 
 */
package de.tuberlin.ise.benchfoundry.physicalschema.model.columnstore;

import java.util.TreeSet;

import junit.framework.TestCase;
import de.tuberlin.ise.benchfoundry.logicalschema.model.Attribute;
import de.tuberlin.ise.benchfoundry.logicalschema.model.DataSchema;
import de.tuberlin.ise.benchfoundry.physicalschema.model.columnstore.CSColumn;
import de.tuberlin.ise.benchfoundry.physicalschema.model.columnstore.CSColumnType;
import de.tuberlin.ise.benchfoundry.physicalschema.model.columnstore.CSCompoundColumn;
import de.tuberlin.ise.benchfoundry.physicalschema.model.columnstore.CSSimpleColumn;

/**
 * @author Dave
 *
 */
public class CSColumnTest extends TestCase {

	static CSColumn simplekey, simpleindex, simplereg, compindex, compreg,
			compkey;
	
	private static String CUSTOMER = "CSCT_customer";
	private static String FNAME = "fname";
	private static String LNAME = "lname";
	private static String ID = "id";
	private static String ADDRESS = "address";

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
				schema.getAttributeByName(ID, CUSTOMER));
		simplereg = new CSSimpleColumn(CSColumnType.REGULAR,
				schema.getAttributeByName(ADDRESS, CUSTOMER));
		compkey = new CSCompoundColumn(CSColumnType.KEY,
				schema.getAttributeByName(FNAME, CUSTOMER),
				schema.getAttributeByName(LNAME, CUSTOMER));
		compindex = new CSCompoundColumn(CSColumnType.INDEX,
				schema.getAttributeByName(FNAME, CUSTOMER),
				schema.getAttributeByName(LNAME, CUSTOMER));
		compreg = new CSCompoundColumn(CSColumnType.REGULAR,
				schema.getAttributeByName(FNAME, CUSTOMER),
				schema.getAttributeByName(LNAME, CUSTOMER));
	}

	/**
	 * Test method for
	 * {@link de.tuberlin.ise.benchfoundry.physicalschema.model.columnstore.CSColumn#getName()}
	 * .
	 */
	public void testGetName() {
		assertEquals("CSCT_customer.fname-CSCT_customer.lname", compkey.getName());
		assertEquals("CSCT_customer.id", simplekey.getName());
	}

	/**
	 * Test method for
	 * {@link de.tuberlin.ise.benchfoundry.physicalschema.model.columnstore.CSColumn#getAttributes()}
	 * .
	 */
	public void testGetAttributes() {
		TreeSet<Attribute> fnamelnameset = new TreeSet<Attribute>(), idset = new TreeSet<Attribute>(), addrset = new TreeSet<Attribute>();
		fnamelnameset.add(DataSchema.getInstance().getAttributeByName(FNAME, CUSTOMER));
		fnamelnameset.add(DataSchema.getInstance().getAttributeByName(LNAME, CUSTOMER));
		idset.add(DataSchema.getInstance().getAttributeByName(ID, CUSTOMER));
		addrset.add(DataSchema.getInstance().getAttributeByName(ADDRESS, CUSTOMER));
		assertEquals(fnamelnameset, compindex.getAttributes());
		assertEquals(fnamelnameset, compkey.getAttributes());
		assertEquals(fnamelnameset, compreg.getAttributes());
		assertEquals(idset, simplekey.getAttributes());
		assertEquals(idset, simpleindex.getAttributes());
		assertEquals(addrset, simplereg.getAttributes());
	}

	/**
	 * Test method for
	 * {@link de.tuberlin.ise.benchfoundry.physicalschema.model.columnstore.CSColumn#compareTo(de.tuberlin.ise.benchfoundry.physicalschema.model.columnstore.CSColumn)}
	 * .
	 */
	public void testCompareTo() {
		TreeSet<CSColumn> cols = new TreeSet<CSColumn>();
		cols.add(simpleindex);
		cols.add(compreg);
		cols.add(compindex);
		cols.add(simplekey);
		cols.add(compreg);
		cols.add(simplereg);
		cols.add(compkey);
		assertEquals("[" + compkey + ", " + simplekey + ", " + compindex + ", "
				+ simpleindex + ", " + simplereg + ", " + compreg + "]",
				cols.toString());
	}

	/**
	 * Test method for
	 * {@link de.tuberlin.ise.benchfoundry.physicalschema.model.columnstore.CSColumn#equals(java.lang.Object)}
	 * .
	 */
	public void testEqualsObject() {
		CSColumn identical = new CSSimpleColumn(CSColumnType.KEY, DataSchema
				.getInstance().getAttributeByName(ID, CUSTOMER));
		assertEquals(identical, simplekey);
		// different attribute
		CSColumn diff = new CSSimpleColumn(CSColumnType.KEY, DataSchema
				.getInstance().getAttributeByName(FNAME, CUSTOMER));
		assertTrue(!diff.equals(simplekey));
		// different types
		diff = new CSSimpleColumn(CSColumnType.REGULAR, DataSchema
				.getInstance().getAttributeByName(ID, CUSTOMER));
		assertTrue(!diff.equals(simplekey));
		diff = new CSSimpleColumn(CSColumnType.INDEX, DataSchema.getInstance()
				.getAttributeByName(ID, CUSTOMER));
		assertTrue(!diff.equals(simplekey));
		// different class (compound vs. simple)
		diff = new CSCompoundColumn(CSColumnType.KEY, DataSchema.getInstance()
				.getAttributeByName(ID, CUSTOMER));
		assertTrue(!diff.equals(simplekey));
	}
}
