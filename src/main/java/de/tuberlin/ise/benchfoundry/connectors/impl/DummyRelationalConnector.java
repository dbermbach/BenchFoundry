/**
 * 
 */
package de.tuberlin.ise.benchfoundry.connectors.impl;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.tuberlin.ise.benchfoundry.connectors.IDbConnector;
import de.tuberlin.ise.benchfoundry.connectors.RelationalDbConnector;
import de.tuberlin.ise.benchfoundry.connectors.exceptions.PrepareTransactionException;
import de.tuberlin.ise.benchfoundry.physicalschema.model.AbstractPhysicalSchema;
import de.tuberlin.ise.benchfoundry.physicalschema.model.relational.RelationalRequest;
import de.tuberlin.ise.benchfoundry.util.BenchFoundryConfigData;
import de.tuberlin.ise.benchfoundry.util.SelectiveLogEntry;

/**
 * @author Dave
 * 
 *         this class can serve as a dummy for local testing. Its methods only
 *         print parameters to the console.
 * 
 * 
 *
 */
public class DummyRelationalConnector extends RelationalDbConnector {

	private static final Logger LOG = LogManager
			.getLogger(DummyRelationalConnector.class);

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * de.tuberlin.ise.benchfoundry.connectors.IDbConnector#setupPhysicalSchema(de
	 * .tuberlin.ise.benchfoundry.physicalschema.model.AbstractPhysicalSchema)
	 */
	@Override
	public void setupPhysicalSchema(AbstractPhysicalSchema schema) {
		LOG.debug("in setupPhysicalSchema()");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * de.tuberlin.ise.benchfoundry.connectors.IDbConnector#executeBusinessTransaction
	 * (long, long)
	 */
	@Override
	public void executeBusinessTransaction(long processId, long transactionId,
			SelectiveLogEntry log) throws Exception {
		if (SelectiveLogEntry.doDetailledLogging)
			log.log(this, "in executeBusinessTransaction");

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.tuberlin.ise.benchfoundry.connectors.IDbConnector#cleanUpDatabase()
	 */
	@Override
	public void cleanUpDatabase() {
		LOG.debug("in cleanUpDatabase()");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.tuberlin.ise.benchfoundry.connectors.DbConnector#init()
	 */
	@Override
	public void init() {
		LOG.debug("in init()");

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.tuberlin.ise.benchfoundry.connectors.DbConnector#cleanup()
	 */
	@Override
	public void cleanup() {
		LOG.debug("in cleanup()");

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.tuberlin.ise.benchfoundry.connectors.IDbConnector#serializeConnector()
	 */
	@Override
	public byte[] serializeConnector() {
		return new byte[0];
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * de.tuberlin.ise.benchfoundry.connectors.IDbConnector#deserializeConnector(
	 * byte[])
	 */
	@Override
	public IDbConnector deserializeConnector(byte[] serializedConnector) {
		return this;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.tuberlin.ise.benchfoundry.connectors.RelationalDbConnector#
	 * prepareRelationalTransaction(long, long, java.util.List, java.util.List,
	 * java.util.List, boolean)
	 */
	@Override
	protected void prepareRelationalTransaction(long processId,
			long transactionId, List<RelationalRequest> operations,
			List<Integer> businessOperationIds, List<List<String>> params,
			boolean doMeasurements, SelectiveLogEntry log)
			throws PrepareTransactionException {
		// LOG.debug("preparing transaction (pId=" + processId + ", txId="
		// + transactionId + ", opIds=" + businessOperationIds + ")");
		if (SelectiveLogEntry.doDetailledLogging)
			log.log(this, "in prepareRelationalTransaction");

	}

}
