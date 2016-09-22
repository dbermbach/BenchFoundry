/**
 * 
 */
package de.tuberlin.ise.benchfoundry.physicalschema.model.columnstore.schemagen;

import de.tuberlin.ise.benchfoundry.logicalschema.queries.LogicalQuery;
import de.tuberlin.ise.benchfoundry.logicalschema.queries.LogicalQueryRegistry;
import de.tuberlin.ise.benchfoundry.physicalschema.model.columnstore.CSPhysicalSchema;
import de.tuberlin.ise.benchfoundry.physicalschema.model.columnstore.CSRequest;

/**
 * translates a set of {@link LogicalQuery} objects into {@link CSRequest}
 * sequences.
 * 
 * @author Dave
 *
 */
public class CSLogicalQueryTranslator {

	/** holds the schema for which we translate the queries */
	private final CSPhysicalSchema schema;

	/**
	 * @param schema
	 */
	public CSLogicalQueryTranslator(CSPhysicalSchema schema) {
		this.schema = schema;
	}

	/**
	 * retrieves a {@link LogicalQuery} objects from the
	 * {@link LogicalQueryRegistry} and determines the sequence of
	 * {@link CSRequest}s necessary to execute that query against a column
	 * store. registers each sequence of requests with the schema object.
	 * 
	 * 
	 */
	public void translateQueries() {
		// TODO translate all queries one by one...

	}

}
