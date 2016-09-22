/**
 * 
 */
package de.tuberlin.ise.benchfoundry.physicalschema.model.columnstore;

import java.util.Collection;
import java.util.SortedSet;

import de.tuberlin.ise.benchfoundry.logicalschema.model.Attribute;

/**
 * @author Dave
 *
 */
public abstract class CSColumn implements Comparable<CSColumn> {

	/**
	 * whether this column is the row key, a secondary index column, or a
	 * "regular" column
	 */
	protected final CSColumnType type;

	/**
	 * @param type
	 */
	protected CSColumn(CSColumnType type) {
		super();
		this.type = type;
	}

	/**
	 * 
	 * @return a String representation of the name of the column. Should
	 *         included both the name of the attribute as well as of the
	 *         belonging entity
	 */
	public abstract String getName();

	/**
	 * 
	 * @return a String representation of the name of the column. Should include
	 *         only the name of the attribute(s).
	 */
	public abstract String getSimpleName();

	/**
	 * 
	 * @return the corresponding attribute(s) of this column.
	 */
	public abstract SortedSet<Attribute> getAttributes();

	/**
	 * 
	 * @return a deep copy of this object.
	 */
	public abstract CSColumn deepClone();

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return getName() + "(" + type + ")";
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(CSColumn other) {
		if (toString().equals(other.toString()))
			return 0;
		if (type.compareTo(other.type) == 0)
			return getName().compareTo(other.getName());
		return type.compareTo(other.type);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return toString().hashCode();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object other) {
		if (other == null)
			return false;
		if (!(this.getClass().equals(other.getClass())))
			return false;
		return toString().equals(((CSColumn) other).toString());

	}

	/**
	 * 
	 * @param type
	 * @param attributes
	 * @return a new instance of either {@link CSSimpleColumn} or
	 *         {@link CSCompoundColumn} depending on the number of attributes
	 *         provided. Returns null if no attribute is provided.
	 */
	public static CSColumn createCSColumn(CSColumnType type,
			Attribute... attributes) {
		if (attributes.length == 0 || attributes[0] == null)
			return null;
		if (attributes.length == 1)
			return new CSSimpleColumn(type, attributes[0]);
		return new CSCompoundColumn(type, attributes);
	}

	/**
	 * 
	 * @param type
	 * @param attributes
	 * @return a new instance of either {@link CSSimpleColumn} or
	 *         {@link CSCompoundColumn} depending on the number of attributes
	 *         provided. Returns null if no attribute is provided.
	 */
	public static CSColumn createCSColumn(CSColumnType type,
			Collection<Attribute> attributes) {
		if (attributes.size() == 0)
			return null;
		if (attributes.size() == 1)
			return new CSSimpleColumn(type, attributes.iterator().next());
		return new CSCompoundColumn(type, attributes);
	}

}
