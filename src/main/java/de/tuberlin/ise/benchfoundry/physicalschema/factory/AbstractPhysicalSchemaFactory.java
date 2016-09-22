/**
 * 
 */
package de.tuberlin.ise.benchfoundry.physicalschema.factory;

import de.tuberlin.ise.benchfoundry.physicalschema.model.AbstractPhysicalSchema;

/**
 * abstract factory pattern: subclasses create instances of subclasses of
 * {@link AbstractPhysicalSchema}. The instance creation does not only involve
 * the creation of a physical schema but also involves the translation of all
 * queries to lists of requests.
 * 
 * @author Dave
 *
 */
public abstract class AbstractPhysicalSchemaFactory {

	/**
	 * creates an instance of a concrete physical schema including all the
	 * number crunching necessary to do so. Also translates all queries to their
	 * corresponding List<AbstractRequest> and adds them to
	 * schemaInstance.requests
	 * 
	 * @return an instance of {@link AbstractPhysicalSchema}, e.g.,
	 *         ColumnStorePhysicalSchema
	 */
	public abstract AbstractPhysicalSchema createPhysicalSchema();

	/**
	 * @param schema
	 *            the schema which shall be serialized
	 * 
	 * @return a serialized version of schemas created by this factory or null
	 *         if an error occurs
	 */
	public abstract byte[] serializeSchema(AbstractPhysicalSchema schema);

	/**
	 * 
	 * @param serializedSchema
	 *            a serialized version of schemas created by this class
	 * @return the schema which was serialized by a different instance of the
	 *         same factory class or null if an error occurs.
	 */
	public abstract AbstractPhysicalSchema deserializeSchema(
			byte[] serializedSchema);

}
