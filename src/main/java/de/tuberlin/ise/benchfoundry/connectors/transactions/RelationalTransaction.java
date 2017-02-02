package de.tuberlin.ise.benchfoundry.connectors.transactions;

import java.util.ArrayList;
import java.util.List;

import de.tuberlin.ise.benchfoundry.results.ResultType;
import de.tuberlin.ise.benchfoundry.scheduling.BusinessOperation;
import de.tuberlin.ise.benchfoundry.scheduling.BusinessProcess;
import de.tuberlin.ise.benchfoundry.scheduling.BusinessTransaction;

/**
 * 
 * @author joernkuhlenkamp
 *
 */
public class RelationalTransaction {

	/** ID of {@link BusinessProcess} */
	protected final long processId;

	/** ID of {@link BusinessTransaction} */
	protected final long transactionId;

	/**
	 * Flag that indicates if this {@link BusinessTransaction} should be
	 * executed against a SUT with transactional guarantees
	 */
	protected final boolean hasTransactionalGurantees;

	protected final boolean doMeasurements;
	
	/** Execution start timestamp [msec] of {@link BusinessTransaction} */
	protected long transactionStart;

	/** Execution end timestamp [msec] of {@link BusinessTransaction} */
	protected long transactionEnd;

	/** Status code of the transaction response, e.g., OK, FAILURE, ... */
	protected ResultType transactionStatus = ResultType.SUCCESSFUL; //FIXME actually use this field

	/**
	 * Lookup of {@link RelationalOperation} by ID of {@link BusinessOperation}
	 */
	private final List<RelationalOperation> operations;

	public RelationalTransaction(long processId, long transactionId, boolean hasTransactionalGurantees, boolean doMeasurements) {
		super();
		this.processId = processId;
		this.transactionId = transactionId;
		this.hasTransactionalGurantees = hasTransactionalGurantees;
		this.operations = new ArrayList<>();
		this.doMeasurements = doMeasurements;
	}

	public long getProcessId() {
		return processId;
	}

	public long getTransactionId() {
		return transactionId;
	}

	public void setTransactionStart(long transactionStart) {
		this.transactionStart = transactionStart;
	}

	public void setTransactionEnd(long transactionEnd) {
		this.transactionEnd = transactionEnd;
	}

	public void setResultType(ResultType resultType) {
		this.transactionStatus = resultType;
	}

	public void addOperation(RelationalOperation operation) {
		operations.add(operation);
	}

	public boolean hasTransactionalGurantees() {
		return hasTransactionalGurantees;
	}

	public List<RelationalOperation> getOperations() {
		return operations;
	}

	public boolean doMeasurements() {
		return doMeasurements;
	}

	/**
	 * Logging of all information within this relational transaction to the BM
	 * logger.
	 */
	public void log() {
		for (RelationalOperation o : operations) {
			o.log();
		}
	}

}
