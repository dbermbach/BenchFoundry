/**
 * 
 */
package de.tuberlin.ise.benchfoundry.physicalschema.factory;

import de.tuberlin.ise.benchfoundry.physicalschema.model.AbstractPhysicalSchema;
import de.tuberlin.ise.benchfoundry.physicalschema.model.columnstore.CSPhysicalSchema;
import de.tuberlin.ise.benchfoundry.physicalschema.model.columnstore.schemagen.CSLogicalQueryTranslator;
import de.tuberlin.ise.benchfoundry.physicalschema.model.columnstore.schemagen.CSSchemaOptionChooser;
import de.tuberlin.ise.benchfoundry.physicalschema.model.columnstore.schemagen.CSSchemaOptionGenerator;

/**
 * @author Dave
 *
 */
public class CSPhysicalSchemaFactory extends AbstractPhysicalSchemaFactory {

	/* (non-Javadoc)
	 * @see de.tuberlin.ise.benchfoundry.physicalschema.factory.AbstractPhysicalSchemaFactory#createPhysicalSchema()
	 */
	@Override
	public AbstractPhysicalSchema createPhysicalSchema() {
		CSPhysicalSchema schema = new CSPhysicalSchema();
		CSSchemaOptionGenerator generator = new CSSchemaOptionGenerator();
		generator.generateSchemaOptions();
		CSSchemaOptionChooser chooser = new CSSchemaOptionChooser(generator); 
		//FIXME add a class reference to some ranking method
		chooser.selectOptionAndFillSchema(schema);
		CSLogicalQueryTranslator queryTranslator = new CSLogicalQueryTranslator(schema);
		queryTranslator.translateQueries();
		return schema;
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.ise.benchfoundry.physicalschema.factory.AbstractPhysicalSchemaFactory#serializeSchema(de.tuberlin.ise.benchfoundry.physicalschema.model.AbstractPhysicalSchema)
	 */
	@Override
	public byte[] serializeSchema(AbstractPhysicalSchema schema) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.ise.benchfoundry.physicalschema.factory.AbstractPhysicalSchemaFactory#deserializeSchema(byte[])
	 */
	@Override
	public AbstractPhysicalSchema deserializeSchema(byte[] serializedSchema) {
		// TODO Auto-generated method stub
		return null;
	}

}
