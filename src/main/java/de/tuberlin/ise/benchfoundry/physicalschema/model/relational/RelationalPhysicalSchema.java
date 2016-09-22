/**
 * 
 */
package de.tuberlin.ise.benchfoundry.physicalschema.model.relational;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;

import de.tuberlin.ise.benchfoundry.physicalschema.model.AbstractPhysicalSchema;
import de.tuberlin.ise.benchfoundry.util.BenchFoundryConfigData;
import de.tuberlin.ise.benchfoundry.util.QueryInputFileParser;

/**
 * @author Dave
 *
 */
public class RelationalPhysicalSchema extends AbstractPhysicalSchema {

	/**
	 * 
	 * @return all original SQL DDL statements or null if an error occured while
	 *         reading
	 */
	public Map<Integer, String> getTableCreationStatements() {
		try {
			return QueryInputFileParser
					.readQueryInputFileFromStream(new FileInputStream(
							BenchFoundryConfigData.schemaFilename));
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}

}
