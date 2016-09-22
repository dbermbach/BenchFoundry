/**
 *
 */
package de.tuberlin.ise.benchfoundry.logicalschema.queries;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.file.Paths;

import de.tuberlin.ise.benchfoundry.logicalschema.model.DataSchema;
import de.tuberlin.ise.benchfoundry.logicalschema.queries.LogicalQueryRegistry;
import junit.framework.TestCase;

/**
 *
 * @author Akon Dey (akon.dey@sydney.edu.au)
 */
public class LogicalQueryRegistryTest extends TestCase {

	/* (non-Javadoc)
	 * @see junit.framework.TestCase#setUp()
	 */
	protected void setUp() throws Exception {
		super.setUp();
		DataSchema.getInstance().addSchemaInputFile("samples/schema.txt");
	}

	/* (non-Javadoc)
	 * @see junit.framework.TestCase#tearDown()
	 */
	protected void tearDown() throws Exception {
		super.tearDown();
	}

	/**
	 * Test method for {@link de.tuberlin.ise.benchfoundry.logicalschema.queries.LogicalQueryRegistry#getInstance()}.
	 */
	public final void testGetInstance() {
		assertNotNull(LogicalQueryRegistry.getInstance());
	}

	/**
	 * Test method for {@link de.tuberlin.ise.benchfoundry.logicalschema.queries.LogicalQueryRegistry#addQueryInputFile(InputStream)}
	 */
	public final void testAddQueryInputFileStream() {

		try {
			// store the query input file in a string
			String queries = "1:SELECT id, name, address FROM customer;";

			// convert query String into InputStream
			InputStream is = new ByteArrayInputStream(queries.getBytes());
			
			// register the query to the LogicalQueryRegistry
			LogicalQueryRegistry.getInstance().addQueryInputFile(is);
			
			is.close();
		} catch (Exception e) {
			fail("Unable to parse query");
		}
	}

	/**
	 * Test method for {@link de.tuberlin.ise.benchfoundry.logicalschema.queries.LogicalQueryRegistry#addQueryInputFile(String)}
	 */
	public final void testAddQueryInputFile() {

		try {
			// register the queries into the LogicalQueryRegistry
			LogicalQueryRegistry.getInstance().addQueryInputFile("samples/oplist_example.txt");
		} catch (Exception e) {
			e.printStackTrace();
			fail("Unable to parse query");
		}
	}
	
	/**
	 * Test method for {@link de.tuberlin.ise.benchfoundry.logicalschema.queries.LogicalQueryRegistry#getQueryForId(int)}.
	 */
	public final void testGetQueryForId() {
		assertNotNull(LogicalQueryRegistry.getInstance().getQueryForId(1));
	}

}
