/**
 * 
 */
package de.tuberlin.ise.benchfoundry.physicalschema.model.columnstore.schemagen;

import de.tuberlin.ise.benchfoundry.physicalschema.model.columnstore.CSPhysicalSchema;

/**
 * 
 * ranks a set of schema options for a column store and chooses the
 * "best fitting" one for the respective use case.
 * 
 * @author Dave
 *
 */
public class CSSchemaOptionChooser {

	/** has determined a set of schema options, allows access to these options */
	private CSSchemaOptionGenerator generator;

	/**
	 * @param generator
	 *            the {@link CSSchemaOptionGenerator} which has determined a set
	 *            of schema options and allows access to these options
	 */
	public CSSchemaOptionChooser(CSSchemaOptionGenerator generator) {
		this.generator = generator;
	}

	/**
	 * @param schema
	 */
	public void selectOptionAndFillSchema(CSPhysicalSchema schema) {
		// TODO take the selected schema option and make sure that it is
		// reflected in the schema object

	}

}
