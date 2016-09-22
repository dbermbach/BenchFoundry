/**
 * 
 */
package de.tuberlin.ise.benchfoundry.logicalschema.model;

/**
 * describes attributes of entities (e.g., color, size, ...)
 * 
 * @author Dave
 *
 */
public class Attribute implements Comparable<Attribute> {

	/** the entity to which this attribute belongs */
	private final Entity entity;

	/** name of the attribute */
	private final String name;

	/**
	 * reference to another entity owned by this entity - e.g., a customer has
	 * orders.
	 */
	private Entity other;

	/**
	 * if other != null, this specifies the cardinality of the other entity,
	 * i.e., a customer has MANY orders but an order belongs to only ONE
	 * customer
	 */
	private RelationshipType cardinalityOther;

	/**
	 * if other != null, this specifies the cardinality of this entity, i.e., a
	 * customer has MANY orders but an order belongs to only ONE customer
	 */
	private RelationshipType cardinalitySelf;

	/**
	 * @param entity
	 *            entity to which this attribute belongs
	 * @param name
	 *            name of the attribute
	 */
	public Attribute(Entity entity, String name) {
		super();
		this.entity = entity;
		this.name = name;
	}

	/**
	 * sets a relationship to another entity if not already set. Example: If
	 * this attribute belongs to a Customer entity, then other might be Order
	 * and cardinality might be MANY since a customer may have more than one
	 * order.
	 * 
	 * @param other
	 *            the other entity
	 * @param cardinality
	 *            cardinality of the target entity
	 * @param ownCardinality
	 *            cardinality of the entity this attribute belongs to
	 * @return true if successfully set, false when either parameter is null or
	 *         has already been set or when other is identical to the
	 *         surrounding entity (self-reference)
	 */
	boolean addRelationship(Entity other, RelationshipType cardinality,
			RelationshipType ownCardinality) {
		if (this.other == null && other != null && other != entity
				&& cardinality != null && ownCardinality != null) {
			this.other = other;
			this.cardinalityOther = cardinality;
			this.cardinalitySelf = ownCardinality;
			return true;
		}
		return false;
	}

	/**
	 * drops any existing relationships
	 */
	void removeRelationship() {
		other = null;
		cardinalityOther = null;
		cardinalitySelf = null;
	}

	/**
	 * @return the entity
	 */
	public Entity getEntity() {
		return this.entity;
	}

	/**
	 * @return the name
	 */
	public String getName() {
		return this.name;
	}

	/**
	 * 
	 * @return "entityname.attributename"
	 */
	public String getExtendedName() {
		return entity.getName() + "." + name;
	}

	/**
	 * @return the other
	 */
	public Entity getOther() {
		return this.other;
	}

	/**
	 * the cardinality of the other entity (e.g., if this is Customer and other
	 * is Order, then the value will be MANY)
	 * 
	 * @return the cardinalityOther
	 */
	public RelationshipType getCardinalityOther() {
		return this.cardinalityOther;
	}

	/**
	 *  the cardinality of the entity this attribute belongs to (e.g., if this is Customer and other
	 * is Order, then the value will be ONE)
	 * 
	 * @return
	 */
	public RelationshipType getCardinalitySelf() {
		return this.cardinalitySelf;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		if (other == null)
			return name;
		else
			return name + "(ref:" + other.getName() + ",card:"
					+ cardinalityOther + ")";
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
		result = prime
				* result
				+ ((this.entity == null) ? 0 : this.entity.getName().hashCode());
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
		if (!(obj instanceof Attribute))
			return false;
		Attribute other = (Attribute) obj;
		if (this.entity == null || other.entity == null) {
			return false;
		} else if (!this.entity.getName().equals(other.entity.getName()))
			return false;
		if (this.name == null || other.name == null) {
			return false;
		} else
			return this.name.equals(other.name);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(Attribute other) {
		if (this.equals(other))
			return 0;
		if (entity.getName().equals(other.entity.getName()))
			return name.compareTo(other.name);
		return entity.getName().compareTo(other.entity.getName());
	}

}
