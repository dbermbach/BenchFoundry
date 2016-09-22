/**
 * 
 */
package de.tuberlin.ise.benchfoundry.logicalschema.model;

/**
 * describes the relationship of different entities by specifying the
 * cardinality of "owned" other entities
 * 
 * @author Dave
 *
 */
public enum RelationshipType {

	ONE, MANY;

	/**
	 * 
	 * @param rels
	 *            an array of length 2
	 * @return true if both {@link RelationshipType}s are ONE
	 */
	public static boolean isOneToOne(RelationshipType[] rels) {
		return rels[0] == ONE && rels[1] == ONE;
	}

	/**
	 * 
	 * @param rels
	 *            an array of length 2
	 * @return true if one {@link RelationshipType}is ONE and the other is MANY
	 */
	public static boolean isOneToMany(RelationshipType[] rels) {
		return (rels[0] == ONE && rels[1] == MANY)
				|| (rels[0] == MANY && rels[1] == ONE);
	}

	/**
	 * 
	 * @param rels
	 *            an array of length 2
	 * @return true if both {@link RelationshipType}s are MANY
	 */
	public static boolean isManyToMany(RelationshipType[] rels) {
		return rels[0] == MANY && rels[1] == MANY;
	}

}
