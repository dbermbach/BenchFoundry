/**
 * 
 */
package de.tuberlin.ise.benchfoundry.logicalschema.model;

import java.util.TreeSet;

import de.tuberlin.ise.benchfoundry.logicalschema.model.Attribute;
import de.tuberlin.ise.benchfoundry.logicalschema.model.Entity;
import de.tuberlin.ise.benchfoundry.logicalschema.model.RelationshipType;
import junit.framework.TestCase;

/**
 * @author Dave
 *
 */
public class AttributeTest extends TestCase {

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
	 * {@link de.tuberlin.ise.benchfoundry.logicalschema.model.Attribute#hashCode()}.
	 */
	public void testHashCode() {
		Attribute custFname = new Attribute(new Entity("customer"), "fname");
		Attribute identical = new Attribute(new Entity("customer"), "fname");
		assertTrue(identical.hashCode() == custFname.hashCode());
		Attribute different = new Attribute(new Entity("entity"), "fname");
		assertTrue(different.hashCode() != custFname.hashCode());
		different = new Attribute(new Entity("customer"), "attributename");
		assertTrue(different.hashCode() != custFname.hashCode());
		different = new Attribute(new Entity("entity"), "attributename");
		assertTrue(different.hashCode() != custFname.hashCode());
	}

	/**
	 * Test method for
	 * {@link de.tuberlin.ise.benchfoundry.logicalschema.model.Attribute#addRelationship(de.tuberlin.ise.benchfoundry.logicalschema.model.Entity, de.tuberlin.ise.benchfoundry.logicalschema.model.RelationshipType)}
	 * .
	 */
	public void testAddRelationship() {
		Entity e = new Entity("e"), e2 = new Entity("e2");
		Attribute a = new Attribute(e, "a");
		Attribute a2 = new Attribute(e2, "a");
		assertTrue(a.addRelationship(e2, RelationshipType.MANY,RelationshipType.MANY));
		assertTrue(!(a.addRelationship(new Entity("some entity"),
				RelationshipType.MANY,RelationshipType.MANY)));
		assertTrue(!(a2.addRelationship(e2, RelationshipType.MANY,RelationshipType.MANY)));
		assertEquals(e2, a.getOther());
		assertTrue(a.getCardinalityOther()==RelationshipType.MANY);
	}

	/**
	 * Test method for
	 * {@link de.tuberlin.ise.benchfoundry.logicalschema.model.Attribute#removeRelationship()}
	 * .
	 */
	public void testRemoveRelationship() {
		Entity e = new Entity("e"), e2 = new Entity("e2");
		Attribute a = new Attribute(e, "a");
		a.addRelationship(e2, RelationshipType.MANY,RelationshipType.MANY);
		a.removeRelationship();
		assertNull(a.getCardinalityOther());
		assertNull(a.getOther());
	}

	/**
	 * Test method for
	 * {@link de.tuberlin.ise.benchfoundry.logicalschema.model.Attribute#getExtendedName()}
	 * .
	 */
	public void testGetExtendedName() {
		Attribute custFname = new Attribute(new Entity("customer"), "fname");
		assertEquals("customer.fname", custFname.getExtendedName());
	}

	/**
	 * Test method for
	 * {@link de.tuberlin.ise.benchfoundry.logicalschema.model.Attribute#equals(java.lang.Object)}
	 * .
	 */
	public void testEqualsObject() {
		Attribute custFname = new Attribute(new Entity("customer"), "fname");
		Attribute identical = new Attribute(new Entity("customer"), "fname");
		assertTrue(identical.equals(custFname));
		Attribute different = new Attribute(new Entity("entity"), "fname");
		assertTrue(!different.equals(custFname));
		different = new Attribute(new Entity("customer"), "attributename");
		assertTrue(!different.equals(custFname));
		different = new Attribute(new Entity("entity"), "attributename");
		assertTrue(!different.equals(custFname));

	}

	/**
	 * Test method for
	 * {@link de.tuberlin.ise.benchfoundry.logicalschema.model.Attribute#compareTo(de.tuberlin.ise.benchfoundry.logicalschema.model.Attribute)}
	 * .
	 */
	public void testCompareTo() {
		Attribute first = new Attribute(new Entity("AEntity"), "AAttribute");
		Attribute second = new Attribute(new Entity("AEntity"), "BAttribute");
		Attribute third = new Attribute(new Entity("BEntity"), "AAttribute");
		Attribute fourth = new Attribute(new Entity("BEntity"), "BAttribute");
		TreeSet<Attribute> set = new TreeSet<Attribute>();
		set.add(first);
		set.add(second);
		set.add(third);
		set.add(fourth);
		String s = "";
		for (Attribute a : set)
			s += a.getExtendedName();
		assertEquals(
				"AEntity.AAttributeAEntity.BAttributeBEntity.AAttributeBEntity.BAttribute",
				s);
	}

}
