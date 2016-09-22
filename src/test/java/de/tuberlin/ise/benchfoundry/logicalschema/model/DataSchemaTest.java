/**
 * 
 */
package de.tuberlin.ise.benchfoundry.logicalschema.model;

import de.tuberlin.ise.benchfoundry.logicalschema.model.Attribute;
import de.tuberlin.ise.benchfoundry.logicalschema.model.DataSchema;
import de.tuberlin.ise.benchfoundry.logicalschema.model.RelationshipType;
import junit.framework.TestCase;

/**
 * @author Dave
 *
 */
public class DataSchemaTest extends TestCase {

	static DataSchema schema = DataSchema.getInstance();

	/*
	 * (non-Javadoc)
	 * 
	 * @see junit.framework.TestCase#setUp()
	 */
	protected void setUp() throws Exception {
		super.setUp();

		schema.createNewEntity("customer", "fname", "lname", "id", "orders");
		schema.createNewEntity("order", "item", "quantity", "order_id",
				"cust_id");
		schema.addRelationship("customer", "order", "orders", "cust_id",
				RelationshipType.ONE, RelationshipType.MANY);
		schema.setReadyForUse();
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
	 * {@link de.tuberlin.ise.benchfoundry.logicalschema.model.DataSchema#addRelationship(java.lang.String, java.lang.String, java.lang.String, java.lang.String, de.tuberlin.ise.benchfoundry.logicalschema.model.RelationshipType, de.tuberlin.ise.benchfoundry.logicalschema.model.RelationshipType)}
	 * .
	 */
	public void testAddRelationship() {
		Attribute a = schema.getAttributeByName("orders", "customer");
		Attribute b = schema.getAttributeByName("cust_id", "order");
		assertEquals(RelationshipType.MANY, a.getCardinalityOther());
		assertEquals(RelationshipType.ONE, b.getCardinalityOther());
	}


	
	/**
	 * Test method for
	 * {@link de.tuberlin.ise.benchfoundry.logicalschema.model.DataSchema#getAttributeByName(java.lang.String, java.lang.String)}
	 * .
	 */
	public void testGetAttributeByName() {
		Attribute a = schema.getAttributeByName("id", "customer");
		assertEquals(a.getExtendedName(), "customer.id");
		a = schema.getAttributeByName("id", "order");
		assertNull(a);
		a = schema.getAttributeByName("cust_id", "order");
		assertEquals(a.getExtendedName(), "order.cust_id");
		a = schema.getAttributeByName("quantity", "order");
		assertEquals(a.getExtendedName(), "order.quantity");
	}

}
