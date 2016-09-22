/**
 * 
 */
package de.tuberlin.ise.benchfoundry.connectors;

import java.io.Serializable;
import java.util.List;

import de.tuberlin.ise.benchfoundry.connectors.exceptions.PrepareTransactionException;
import de.tuberlin.ise.benchfoundry.physicalschema.model.AbstractPhysicalSchema;
import de.tuberlin.ise.benchfoundry.physicalschema.model.AbstractRequest;
import de.tuberlin.ise.benchfoundry.results.ResultLogger;
import de.tuberlin.ise.benchfoundry.scheduling.BusinessOperation;
import de.tuberlin.ise.benchfoundry.scheduling.BusinessProcess;
import de.tuberlin.ise.benchfoundry.scheduling.BusinessTransaction;
import de.tuberlin.ise.benchfoundry.util.SelectiveLogEntry;

/**
 * @author Dave
 *
 */
public interface IDbConnector extends Serializable {

	/**
	 * sends the calculated physical schema. Depending on the underlying data
	 * store, schemas need be created upfront or can be used on-the-fly without
	 * specific creation instructions (e.g., through SQL DDL statements).
	 * 
	 * @param schema
	 *            the concrete physical schema to be used
	 */
	public void setupPhysicalSchema(AbstractPhysicalSchema schema);

	/**
	 * 
	 * @param processId
	 *            id of the surrounding {@link BusinessProcess}
	 * @param transactionId
	 *            id of the {@link BusinessTransaction}
	 * @param operationId
	 *            id of the {@link BusinessOperation}
	 * @param operations
	 *            list of business operations inside this transaction (outer
	 *            list) plus list of {@link AbstractRequest} instances that form
	 *            a {@link BusinessOperation} (inner list).
	 * @param businessOperationIds
	 *            the corresponding business operation ids to the lists of
	 *            requests in parameter <code>operations</code>
	 * @param params
	 *            ordered list of parameter sets per {@link BusinessOperation}
	 *            (outer list), ordered list of params for a specific
	 *            {@link BusinessOperation} (inner list)
	 * @param custParams
	 *            ordered list of custom parameter sets per
	 *            {@link BusinessOperation} (outer list), ordered list of
	 *            customer parameters for a specific {@link BusinessOperation}
	 *            (inner list)
	 * @param doMeasurements
	 *            measurement results shall only be logged if this is true.
	 *            Otherwise the transaction is part of a clean-up/warm-up
	 *            activity
	 * @param log
	 *            collects detailed information during execution and prints them
	 *            if any error occurs
	 */
	public void prepareTransaction(long processId, long transactionId,
			List<List<? extends AbstractRequest>> operations,
			List<Integer> businessOperationIds, List<List<String>> params,
			List<List<String>> custParams, boolean doMeasurements,
			SelectiveLogEntry log) throws PrepareTransactionException;

	/**
	 * 
	 * 
	 * @param processId
	 *            id of the surrounding {@link BusinessProcess}
	 * @param transactionId
	 *            id of the {@link BusinessTransaction}
	 * @param log
	 *            collects detailed information during execution and prints them
	 *            if any error occurs
	 * @throws Exception
	 * 
	 */
	public void executeBusinessTransaction(long processId, long transactionId,
			SelectiveLogEntry log) throws Exception;

	/**
	 * drop all test-related content from the database so that it can be reused
	 * for another test run.
	 */
	public void cleanUpDatabase();

	/**
	 * 
	 * @return a serialized version of this connector which can then be sent to
	 *         slaves
	 */
	public byte[] serializeConnector();

	/**
	 * deserializes an {@link IDbConnector} instance serialized by another
	 * instance of the same class.
	 * 
	 * @param serializedConnector
	 *            a byte [] as returned by serializeConnector()
	 * @return an object that is a precise copy of the originally serialized
	 *         object
	 */
	public IDbConnector deserializeConnector(byte[] serializedConnector);

	/**
	 * This method initializes state of the connector
	 */
	public void init();

	/**
	 * THis method cleans up any state of the connector.
	 */
	public void cleanup();

}
