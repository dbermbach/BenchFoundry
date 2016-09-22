/**
 * 
 */
package de.tuberlin.ise.benchfoundry.logicalschema.model;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * describes logical data entities (e.g., customer, book, etc.)
 * 
 *
 * 
 * @author Dave
 *
 */
public class Entity {

	/** name of the entity */
	private final String name;

	/** holds all attributes of this entity, key is the attribute name */
	private final Map<String, Attribute> attributes = new HashMap<String, Attribute>();

	/**
	 * holds all attributes that refer to another entity. This is a subset of
	 * the attributes instance variable.
	 */
	private final Map<String, Attribute> attributesWithRelationship = new HashMap<String, Attribute>();

	/**
	 * @param name
	 */
	public Entity(String name) {
		super();
		this.name = name;
	}

	/** adds a new attribute to this entity */
	void addAttribute(String name) {
		attributes.put(name, new Attribute(this, name));
	}

	/**
	 * adds a relationship to another entity if not already set.
	 * 
	 * @param attributeName
	 *            name of the attribute of this entity
	 * @param other
	 *            the other entity
	 * @param cardinality
	 *            the cardinality of the other entity (e.g., if this is Customer
	 *            and other is Order, then cardinality will be MANY)
	 * @param cardinalitySelf
	 *            the cardinality of this entity (e.g., if this is Customer and
	 *            other is Order, then cardinality will be ONE)
	 * @return true if successfully set, false if already set, either of the
	 *         parameters is null or if other==this
	 */
	boolean addRelationship(String attributeName, Entity other,
			RelationshipType cardinality, RelationshipType cardinalitySelf) {
		Attribute a = attributes.get(attributeName);
		if (a.addRelationship(other, cardinality, cardinalitySelf)) {
			attributesWithRelationship.put(attributeName, a);
			return true;
		}
		return false;
	}

	/**
	 * drops a relationship may throw a {@link RuntimeException} if the
	 * specified attribute does not exist
	 * 
	 * @param attributename
	 * 
	 */
	void dropRelationship(String attributename) {
		Attribute a = attributes.get(attributename);
		if (a == null) {
			throw new RuntimeException(
					"Should never have happened: Did not find"
							+ " the specified attribute for name\""
							+ attributename + "\"");
		}
		a.removeRelationship();
	}

	/**
	 * retrieves an attribute by its name
	 * 
	 * @param name
	 *            name of the attribute
	 * @return
	 * @see java.util.Map#get(java.lang.Object)
	 */
	public Attribute getAttributeByName(String name) {
		return this.attributes.get(name);
	}

	/**
	 * @return the name
	 */
	public String getName() {
		return this.name;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		StringBuffer res = new StringBuffer(name + "[");
		for (Attribute a : attributes.values())
			res.append(a + "|");
		res.deleteCharAt(res.length() - 1);
		res.append("]");
		return res.toString();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((this.attributes == null) ? 0 : this.attributes.hashCode());
		result = prime * result
				+ ((this.name == null) ? 0 : this.name.hashCode());
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!(obj instanceof Entity))
			return false;
		Entity other = (Entity) obj;
		if (this.attributes == null) {
			if (other.attributes != null)
				return false;
		} else if (!this.attributes.equals(other.attributes))
			return false;
		if (this.name == null) {
			if (other.name != null)
				return false;
		} else if (!this.name.equals(other.name))
			return false;
		return true;
	}

	/**
	 * checks whether this entity is in relationship to other.
	 * 
	 * @param other
	 *            another entity
	 * @return null if there is no relationship. Otherwise it returns an array
	 *         with length 2 where index 0 specifies the cardinality on this
	 *         side and index 1 specified the cardinality of the other entity
	 */
	public RelationshipType[] getRelationshipTo(Entity other) {
		for (Attribute a : attributesWithRelationship.values()) {
			if (a.getOther() == other) {
				RelationshipType[] result = new RelationshipType[2];
				result[1] = a.getCardinalityOther();
				result[0] = a.getCardinalitySelf();
				return result;
			}
		}
		return null;
	}

	/**
	 * @return the attributes of this entity
	 */
	public Set<Attribute> getAttributes() {
		return new HashSet<Attribute>(this.attributes.values());
	}

}
