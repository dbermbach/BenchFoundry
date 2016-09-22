package de.tuberlin.ise.benchfoundry.connectors.transactions;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.tuberlin.ise.benchfoundry.results.BusinessOperationResult;
import de.tuberlin.ise.benchfoundry.results.RequestResult;
import de.tuberlin.ise.benchfoundry.results.ResultLogger;
import de.tuberlin.ise.benchfoundry.results.ResultType;
import de.tuberlin.ise.benchfoundry.scheduling.BusinessOperation;

/**
 * 
 * @author joernkuhlenkamp
 *
 */
public class RelationalOperation {

	// private static final Logger LOG = LogManager.getLogger();

	/** Corresponding {@link RelationalTransaction} */
	private final RelationalTransaction t;

	/** ID of {@link BusinessOperation} */
	private final long operationId;

	/** SQL statement of {@link BusinessOperation} */
	private final String operationStmt;

	/** Execution start timestamp [msec] of {@link BusinessOperation} */
	private long operationStart;

	/** Execution end timestamp [msec] of {@link BusinessOperation} */
	private long operationEnd;

	/** Status code of the operation response, e.g., OK, FAILURE, ... */
	private ResultType operationStatus = ResultType.SUCCESSFUL;

	/** The payload included in the response: may be null */
	private List<List<String>> responsePayload;

	/** Logger for obtained results */
	private final ResultLogger logger;

	/** id of the logical query executed as part of this operation */
	private final int logicalQueryId;

	/**
	 * @param operationId
	 *            ID of {@link BusinessOperation}
	 */
	public RelationalOperation(RelationalTransaction transaction, long operationId, String operationStmt,
			int logicalQueryId) {
		super();
		this.t = transaction;
		this.operationId = operationId;
		this.operationStmt = operationStmt;
		this.logicalQueryId = logicalQueryId;
		logger = ResultLogger.getInstance();
		
	}

	public void setOperationStart(long operationStart) {
		this.operationStart = operationStart;
	}

	public void setOperationEnd(long operationEnd) {
		this.operationEnd = operationEnd;
	}

	public void setResponseStatus(ResultType responseStatus) {
		this.operationStatus = responseStatus;
	}

	public void setResponsePayload(List<List<String>> responsePayload) {
		this.responsePayload = responsePayload;
	}

	public long getOperationId() {
		return operationId;
	}

	public String getOperationStmt() {
		return operationStmt;
	}
	
	protected void log() {
		BusinessOperationResult r = new BusinessOperationResult(t.processId, t.transactionId, operationId,
				operationStart, operationEnd, new ArrayList<RequestResult>(), operationStatus, responsePayload,
				logicalQueryId);
		// LOG.debug("pId=" +r.getProcessId()+ ", tId=" +r.getTransactionId()+ ", oId=" +r.getOperationId()+ ", lId=" +r.getLogicalQueryId(), "bgn=" +r.getOperationStart()+", end="+r.getOperationEnd());
		logger.persist(r);
	}

}
