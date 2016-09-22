/**
 * 
 */
package de.tuberlin.ise.benchfoundry.scheduling;

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.tuberlin.ise.benchfoundry.connectors.exceptions.PrepareTransactionException;
import de.tuberlin.ise.benchfoundry.logicalschema.queries.ParameterRegistry;
import de.tuberlin.ise.benchfoundry.physicalschema.model.AbstractRequest;
import de.tuberlin.ise.benchfoundry.util.BenchFoundryConfigData;
import de.tuberlin.ise.benchfoundry.util.SelectiveLogEntry;

/**
 * entity that groups several {@link BusinessOperation} instances. If supported
 * by the datastore, business transactions are executed as transactions. If not
 * supported, they are executed in a "best effort" way and isolation violations
 * etc. are tracked.
 * 
 * @author Dave
 *
 */
public class BusinessTransaction {

	private static final Logger LOG = LogManager
			.getLogger(BusinessTransaction.class);

	/** the ID of this business transaction */
	private final long id;

	/** holds all operations within the scope of this business transaction */
	private final List<BusinessOperation> operations = new ArrayList<BusinessOperation>();

	/** process that this transaction belongs to */
	private BusinessProcess surroundingProcess;

	/**
	 * the number of milliseconds that is to elapse between invocation of
	 * execute() and its actual execution
	 */
	private final long delayBeforeStart;

	/**
	 * @param id
	 *            the ID of this business transaction
	 * @param operations
	 *            all operations within the scope of this business transaction
	 * @param delayBeforeStart
	 *            the number of milliseconds that is to elapse between
	 *            invocation of execute() and its actual execution
	 * 
	 */
	BusinessTransaction(long id, List<BusinessOperation> operations,
			long delayBeforeStart) {
		super();
		this.id = id;
		this.operations.addAll(operations);
		this.delayBeforeStart = delayBeforeStart;
	}

	/**
	 * @return the id
	 */
	public long getId() {
		return this.id;
	}

	/**
	 * @return the operations
	 */
	public List<BusinessOperation> getOperations() {
		return this.operations;
	}

	/**
	 * @return the delayBeforeStart
	 */
	public long getDelayBeforeStart() {
		return this.delayBeforeStart;
	}

	/**
	 * @return the surroundingProcess
	 */
	public BusinessProcess getSurroundingProcess() {
		return this.surroundingProcess;
	}

	/**
	 * @param surroundingProcess
	 *            the surroundingProcess to set
	 */
	public void setSurroundingProcess(BusinessProcess surroundingProcess) {
		this.surroundingProcess = surroundingProcess;
	}

	/**
	 * triggers execution of this transaction in the {@link DbConnector}
	 * 
	 * @param log
	 *            collects detailed information during execution
	 * @throws Exception
	 *
	 */
	void execute(SelectiveLogEntry log) throws Exception {
		if (delayBeforeStart > 0)
			try {
				Thread.sleep(delayBeforeStart);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		try {
			if (SelectiveLogEntry.doDetailledLogging)
				log.log(this, "Executing transaction txId=" + id);
			BenchFoundryConfigData.dbConnector
					.executeBusinessTransaction(surroundingProcess.getId(),
							this.id, log);
		} catch (Exception e) {
			if (SelectiveLogEntry.doDetailledLogging) {
				log.log(this,
						"An exception occured while executing transaction txId="
								+ id);
				throw e;
			} else
				LOG.error(
						"An exception occured while executing transaction txId="
								+ id, e);
		}
	}

	/**
	 * sends a prepare command for this transaction to the {@link DbConnector}
	 * and assembles all necessary information
	 * 
	 * @param doMeasurements
	 *            measurement results shall only be logged if this is true.
	 *            Otherwise the process is part of a clean-up/warm-up activity
	 * @param log
	 *            collects detailed information during execution
	 * @throws PrepareTransactionException
	 */
	void prepare(boolean doMeasurements, SelectiveLogEntry log)
			throws PrepareTransactionException {
		boolean doLog = SelectiveLogEntry.doDetailledLogging;
		if (doLog) {
			log.log(this, "Preparing transaction");
			log.logLocalVariable("txId", id);
			log.logLocalVariable("delayBeforeStart", delayBeforeStart);
		}
		List<List<? extends AbstractRequest>> ops = new ArrayList<>();
		List<List<String>> params = new ArrayList<>();
		List<List<String>> custParams = new ArrayList<>();
		List<Integer> businessOpIds = new ArrayList<>();
		for (BusinessOperation bo : operations) {
			try {
				ops.add(BenchFoundryConfigData.physicalSchema
						.getRequestsForQueryId(bo.getLogicalQueryId()));
				params.add(ParameterRegistry.getInstance().getParamForID(
						bo.getParamsetId()));
				custParams.add(ParameterRegistry.getInstance()
						.getCustomParamForID(bo.getCustParamsetId()));
				businessOpIds.add((int) bo.getId());
			} catch (Exception e) {

				if (doLog) {
					log.log(this,
							"Could not retrieve parameters for operation "
									+ bo.toString(), e);
					log.logLocalVariable("requests",
							BenchFoundryConfigData.physicalSchema
									.getRequestsForQueryId(bo
											.getLogicalQueryId()));
					log.logLocalVariable("params", ParameterRegistry
							.getInstance().getParamForID(bo.getParamsetId()));
					log.logLocalVariable(
							"custparams",
							ParameterRegistry
									.getInstance()
									.getCustomParamForID(bo.getCustParamsetId()));
				} else {
					LOG.error(
							"Could not retrieve parameters for one or more operations in txId="
									+ id + ", processId="
									+ surroundingProcess.getId(), e);
					e.printStackTrace();
				}
				throw e;
			}
		}
		// LOG.debug( "processId=" + surroundingProcess.getId()
		// +": first param copy done");
		try {
			BenchFoundryConfigData.dbConnector.prepareTransaction(
					surroundingProcess.getId(), this.id, ops, businessOpIds,
					params, custParams, doMeasurements, log);
		} catch (PrepareTransactionException e) {
			// LOG.error("Transaction could not be prepared: " + e.getMessage()
			// + "(processId=" + surroundingProcess.getId() + ", txId="
			// + this.id + ")", e);
			// e.printStackTrace();
			if (doLog)
				log.log(this, "Transaction could not be prepared: "+e.getMessage());
			throw e;
		}
		if (doLog)log.log(this, "Transaction prepared.");
	}

	/**
	 * Converts this transaction to string format suitable for printing and
	 * logging.
	 */
	public String toString() {
		String str = "BOT;" + this.delayBeforeStart + "\n";
		for (BusinessOperation b : this.operations) {
			str += b.toString() + "\n";
		}
		str += "EOT\n";
		return str;
	}
}
