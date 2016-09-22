/**
 * 
 */
package de.tuberlin.ise.benchfoundry.physicalschema.model.columnstore;

import java.util.SortedSet;
import java.util.TreeSet;

import de.tuberlin.ise.benchfoundry.logicalschema.model.Attribute;

/**
 * instances of this class store a single attribute
 * 
 * @author Dave
 *
 */
public class CSSimpleColumn extends CSColumn {

	/** attribute stored in this column */
	private Attribute attribute;

	/** representation of attribute as a set, cached for efficiency */
	private SortedSet<Attribute> attributeAsSet = new TreeSet<Attribute>();

	/**
	 * @param type
	 *            type of this column (key, index, regular)
	 * @param attribute
	 *            attribute stored in this column
	 */
	public CSSimpleColumn(CSColumnType type, Attribute attribute) {
		super(type);
		this.attribute = attribute;
		this.attributeAsSet.add(attribute);
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
		return attribute.getExtendedName();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.tuberlin.ise.benchfoundry.physicalschema.model.columnstore.CSColumn#
	 * getAttributes()
	 */
	@Override
	public SortedSet<Attribute> getAttributes() {
		return attributeAsSet;
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.ise.benchfoundry.physicalschema.model.columnstore.CSColumn#deepClone()
	 */
	@Override
	public CSColumn deepClone() {
		return new CSSimpleColumn(this.type, this.attribute);
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.ise.benchfoundry.physicalschema.model.columnstore.CSColumn#getSimpleName()
	 */
	@Override
	public String getSimpleName() {
		return attribute.getName();
	}

}
