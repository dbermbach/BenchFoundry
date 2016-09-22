/**
 * 
 */
package de.tuberlin.ise.benchfoundry.physicalschema.model.columnstore;

/**
 * {@link CSTable}s can have either of the following types: <br>
 * - REGULAR: a schema table generated from read and join queries containing
 * actual data <br>
 * - INSERT_ORIGINAL: if a join on two entities A and B is denormalized into a
 * {@link CSTable} instance and there is also an insert query that targets only
 * entity A, then we need to keep the original entity B so that we can
 * recalculate the join during inserts. Such "original" entity data is kept in
 * {@link CSTable}s of this type <br>
 * -LOOKUP: denormalizing entities into {@link CSTable}s may lead to redundant
 * copies of an entity's attributes. Depending on the filter clauses of update
 * and delete queries it may not be possible to identify all copies directly via
 * a read request. In those cases, additional lookup tables that identify the
 * row keys of the respective tables is required.
 * 
 * 
 * @author Dave
 *
 */
public enum CSTableType {

	REGULAR, INSERT_ORIGINAL, LOOKUP;

}
