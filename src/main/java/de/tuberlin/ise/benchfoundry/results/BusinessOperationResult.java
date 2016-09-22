/**
 * 
 */
package de.tuberlin.ise.benchfoundry.results;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import de.tuberlin.ise.benchfoundry.scheduling.BusinessOperation;
import de.tuberlin.ise.benchfoundry.scheduling.BusinessProcess;
import de.tuberlin.ise.benchfoundry.scheduling.BusinessTransaction;

/**
 * holds information on the result of an executed {@link BusinessOperation}
 * 
 * @author Dave
 *
 */
public class BusinessOperationResult {

	/** Delimiter for csv export. */
	private static String delimiter = ";";

	/** ID of the corresponding {@link BusinessProcess} */
	private final long processId;

	/** ID of the corresponding {@link BusinessTransaction} */
	private final long transactionId;

	/** ID of the corresponding {@link BusinessOperation} */
	private final long operationId;

	/** start timestamp of the corresponding {@link BusinessOperation} */
	private final long operationStart;

	/** end timestamp of the corresponding {@link BusinessOperation} */
	private final long operationEnd;

	/** [2016/08/23 JK] Add id of logical query for later analysis. */
	private final int logicalQueryId;

	/**
	 * holds the results of all sub-requests of this {@link BusinessOperation}
	 */
	private final List<RequestResult> subRequests;

	/** describes whether this operation was successful, a failure, ... */
	private final ResultType resultType;

	/** the final result of the operation, may be null */
	private final List<List<String>> operationResult;

	/**
	 * @param operationId
	 *            ID of the corresponding {@link BusinessOperation}
	 * @param operationStart
	 *            start timestamp of the corresponding {@link BusinessOperation}
	 * @param operationEnd
	 *            end timestamp of the corresponding {@link BusinessOperation}
	 * @param subRequests
	 *            results of all sub-requests of this {@link BusinessOperation}
	 * @param resultType
	 *            whether this operation was successful, a failure, ...
	 * @param operationResult
	 *            the final result of the operation, may be null
	 */
	public BusinessOperationResult(long processId, long transactionId, long operationId, long operationStart,
			long operationEnd, List<RequestResult> subRequests, ResultType resultType,
			List<List<String>> operationResult, int logicalQueryId) {
		super();
		this.processId = processId;
		this.transactionId = transactionId;
		this.operationId = operationId;
		this.operationStart = operationStart;
		this.operationEnd = operationEnd;
		this.subRequests = subRequests;
		this.resultType = resultType;
		this.operationResult = operationResult;
		this.logicalQueryId = logicalQueryId;
	}

	
	public long getProcessId() {
		return processId;
	}


	public long getTransactionId() {
		return transactionId;
	}


	/**
	 * @return the operationId
	 */
	public long getOperationId() {
		return this.operationId;
	}

	/**
	 * @return the operationStart
	 */
	public long getOperationStart() {
		return this.operationStart;
	}

	/**
	 * @return the operationEnd
	 */
	public long getOperationEnd() {
		return this.operationEnd;
	}

	/**
	 * @return the subRequests
	 */
	public List<RequestResult> getSubRequests() {
		return this.subRequests;
	}

	/**
	 * @return the resultType
	 */
	public ResultType getResultType() {
		return this.resultType;
	}

	/**
	 * @return the operationResult
	 */
	public List<List<String>> getOperationResult() {
		return this.operationResult;
	}

	/**
	 * @return the logicalQueryId
	 */
	public int getLogicalQueryId() {
		return logicalQueryId;
	}

	public String toCsv() {
		String s = "";
		s += processId + (delimiter);
		s += transactionId + (delimiter);
		s += operationId + (delimiter);
		s += logicalQueryId + (delimiter); 
		s += operationStart + (delimiter);
		s += operationEnd + (delimiter);
		s += resultType.toString() + (delimiter);
		s += "[";
		for (int i = 0; i < operationResult.size(); i++) {
			s += "[";
			for (int j = 0; j < operationResult.get(i).size(); j++) {
				if (j != (operationResult.get(i).size() - 1)) {
					s += operationResult.get(i).get(j) + ',';
				} else {
					s += operationResult.get(i).get(j);
				}
			}
			if (i != (operationResult.get(i).size())) {
				s += "], ";
			} else {
				s += ']';
			}
		}
		s += "]";
		return s;
	}

	public static BusinessOperationResult fromCsv(String line) {
		String tokens[] = line.split(delimiter);
		long processId = Long.parseLong(tokens[0]);
		long transactionId = Long.parseLong(tokens[1]);
		long operationId = Long.parseLong(tokens[2]);
		int logicalQueryId = Integer.parseInt(tokens[3]);
		long operationStart = Long.parseLong(tokens[4]);
		long operationEnd = Long.parseLong(tokens[5]);

		String ops[] = tokens[6].split("[\\[\\]]+");
		List<List<String>> operationResult = new ArrayList<List<String>>();
		for (int i = 1; i < ops.length; i++) {
			List<String> op = new ArrayList<String>();
			op.addAll(Arrays.asList(ops[i].split(",")));
			operationResult.add(op);
		}
		return new BusinessOperationResult(processId, transactionId, operationId, operationStart, operationEnd,
				new ArrayList<RequestResult>(), ResultType.SUCCESSFUL, operationResult, logicalQueryId);
	}
}
