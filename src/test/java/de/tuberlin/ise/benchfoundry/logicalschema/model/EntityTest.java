/**
 * 
 */
package de.tuberlin.ise.benchfoundry.logicalschema.model;

import de.tuberlin.ise.benchfoundry.logicalschema.model.Attribute;
import de.tuberlin.ise.benchfoundry.logicalschema.model.Entity;
import de.tuberlin.ise.benchfoundry.logicalschema.model.RelationshipType;
import junit.framework.TestCase;

/**
 * @author Dave
 *
 */
public class EntityTest extends TestCase {

	Entity e = new Entity("customer");
	Attribute a = new Attribute(e, "fname");
	Attribute b = new Attribute(e, "lname");

	/*
	 * (non-Javadoc)
	 * 
	 * @see junit.framework.TestCase#setUp()
	 */
	protected void setUp() throws Exception {
		super.setUp();

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
	 * {@link de.tuberlin.ise.benchfoundry.logicalschema.model.Entity#hashCode()}.
	 */
	public void testHashCode() {
		e.addAttribute("fname");
		e.addAttribute("lname");
		e.addAttribute("id");
		Entity otherhash = new Entity("othername");
		otherhash.addAttribute("fname");
		otherhash.addAttribute("lname");
		otherhash.addAttribute("id");
		Entity samehash = new Entity("customer");
		samehash.addAttribute("fname");
		samehash.addAttribute("lname");
		samehash.addAttribute("id");
		assertTrue(e.hashCode() != otherhash.hashCode());
		assertTrue(e.hashCode() == samehash.hashCode());

	}

	/**
	 * Test method for
	 * {@link de.tuberlin.ise.benchfoundry.logicalschema.model.Entity#addAttribute(java.lang.String)}
	 * .
	 */
	public void testAddAttribute() {
		e.addAttribute("id");
		Attribute attr = e.getAttributeByName("id");
		assertNotNull(attr);
		assertNotSame(a, attr);
		e.addAttribute("fname");
		attr = e.getAttributeByName("fname");
		assertNotNull(attr);
		assertEquals(a, attr);
	}

	/**
	 * Test method for
	 * {@link de.tuberlin.ise.benchfoundry.logicalschema.model.Entity#addRelationship(java.lang.String, de.tuberlin.ise.benchfoundry.logicalschema.model.Entity, de.tuberlin.ise.benchfoundry.logicalschema.model.RelationshipType)}
	 * .
	 */
	public void testAddRelationship() {
		Entity order = new Entity("order");
		order.addAttribute("cust_id");
		order.addAttribute("item");
		order.addAttribute("qty");
		e.addAttribute("orders");
		e.addRelationship("orders", order, RelationshipType.MANY,RelationshipType.ONE);
		assertTrue(e.getAttributeByName("orders").getCardinalityOther() == RelationshipType.MANY);

	}

	/**
	 * Test method for
	 * {@link de.tuberlin.ise.benchfoundry.logicalschema.model.Entity#dropRelationship(java.lang.String)}
	 * .
	 */
	public void testDropRelationship() {
		Entity order = new Entity("order");
		order.addAttribute("cust_id");
		order.addAttribute("item");
		order.addAttribute("qty");
		e.addAttribute("orders");
		e.addRelationship("orders", order, RelationshipType.MANY,RelationshipType.ONE);
		e.dropRelationship("orders");
		assertNull(e.getAttributeByName("orders").getCardinalityOther());
	}

	/**
	 * Test method for
	 * {@link de.tuberlin.ise.benchfoundry.logicalschema.model.Entity#toString()}.
	 */
	public void testToString() {
		e.addAttribute("fname");
		e.addAttribute("lname");
		assertEquals("customer[fname|lname]", e.toString());
	}

	/**
	 * Test method for
	 * {@link de.tuberlin.ise.benchfoundry.logicalschema.model.Entity#equals(java.lang.Object)}
	 * .
	 */
	public void testEqualsObject() {
		Entity order = new Entity("order");
		order.addAttribute("cust_id");
		e.addAttribute("fname");
		e.addAttribute("lname");
		assertTrue(!e.equals(order));
		Entity dummy = new Entity("almostcustomer");
		dummy.addAttribute("fname");
		dummy.addAttribute("lname");
		assertTrue(!e.equals(dummy));
		Entity identical = new Entity("customer");
		identical.addAttribute("lname");
		identical.addAttribute("fname");
		assertTrue(e.equals(identical));
	}

}
