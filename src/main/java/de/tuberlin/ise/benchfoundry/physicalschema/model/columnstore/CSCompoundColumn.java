/**
 * 
 */
package de.tuberlin.ise.benchfoundry.physicalschema.model.columnstore;

import java.util.Collection;
import java.util.SortedSet;
import java.util.TreeSet;

import de.tuberlin.ise.benchfoundry.logicalschema.model.Attribute;

/**
 * {@link CSCompoundColumn}s store the concatenation of several attributes or
 * use native datastore features like the compound keys in Cassandra.
 * 
 * @author Dave
 *
 */
public class CSCompoundColumn extends CSColumn {

	/** the attributes stored in this column */
	private final SortedSet<Attribute> attributes = new TreeSet<Attribute>();

	/** String representation of attributes, cached for efficiency */
	private String name;
	
	/**same as name but without entity names*/
	private String simpleName;

	/**
	 * character used to separate attribute names in String representations of
	 * this column
	 */
	private static final char separator = '-';

	/**
	 * @param type
	 *            type of this column (key, index, regular)
	 * @param attributes
	 *            attributes stored in this column
	 * 
	 */
	public CSCompoundColumn(CSColumnType type,
			Collection<? extends Attribute> attributes) {
		super(type);
		this.attributes.addAll(attributes);
		updateNameField();
	}

	/**
	 * @param type
	 *            type of this column (key, index, regular)
	 * @param attributes
	 *            attributes stored in this column
	 * 
	 */
	public CSCompoundColumn(CSColumnType type, Attribute... attributes) {
		super(type);
		for (Attribute a : attributes) {
			this.attributes.add(a);
		}
		updateNameField();

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * de.tuberlin.ise.benchfoundry.physicalschema.model.columnstore.CSColumn#getName
	 * ()
	 */
	@Override
	public String getName() {
		return name;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.tuberlin.ise.benchfoundry.physicalschema.model.columnstore.CSColumn#
	 * getAttributes()
	 */
	@Override
	public SortedSet<Attribute> getAttributes() {
		return attributes;
	}

	/**
	 * updates the name field
	 */
	private void updateNameField() {
		name = "";
		simpleName ="";
		for (Attribute a : attributes) {
			name += separator + a.getExtendedName();
			simpleName += separator + a.getName();
		}
		this.name = name.substring(1);
		this.simpleName = simpleName.substring(1);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * de.tuberlin.ise.benchfoundry.physicalschema.model.columnstore.CSColumn#deepClone
	 * ()
	 */
	@Override
	public CSColumn deepClone() {
		return new CSCompoundColumn(this.type, this.attributes);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.tuberlin.ise.benchfoundry.physicalschema.model.columnstore.CSColumn#
	 * getSimpleName()
	 */
	@Override
	public String getSimpleName() {
		return this.simpleName;
	}

}
