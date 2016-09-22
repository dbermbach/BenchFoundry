/**
 * 
 */
package de.tuberlin.ise.benchfoundry.results;

import java.util.List;

import de.tuberlin.ise.benchfoundry.scheduling.BusinessOperation;

/**
 * describes the outcome of a sub-request that is part of a
 * {@link BusinessOperation}.
 * 
 * @author Dave
 *
 */
public class RequestResult {

	/** start timestamp of the request */
	private long requestStart;

	/** end timestamp of the request */
	private long requestEnd;

	/** String representation of the request run against the datastore */
	private String requestDescription;

	/** describes whether this request was successful, a failure, ... */
	private ResultType resultType;

	/** the final result of the operation, may be null */
	private List<List<String>> requestResult;

	/**
	 * @param requestStart
	 *            start timestamp of the request
	 * @param requestEnd
	 *            end timestamp of the request
	 * @param requestDescription
	 *            String representation of the request run against the datastore
	 * @param resultType
	 *            describes whether this request was successful, a failure, ...
	 * @param requestResult
	 *            the final result of the operation, may be null
	 */
	public RequestResult(long requestStart, long requestEnd,
			String requestDescription, ResultType resultType,
			List<List<String>> requestResult) {
		super();
		this.requestStart = requestStart;
		this.requestEnd = requestEnd;
		this.requestDescription = requestDescription;
		this.resultType = resultType;
		this.requestResult = requestResult;
	}

	/**
	 * @return the requestStart
	 */
	public long getRequestStart() {
		return this.requestStart;
	}

	/**
	 * @param requestStart
	 *            the requestStart to set
	 */
	public void setRequestStart(long requestStart) {
		this.requestStart = requestStart;
	}

	/**
	 * @return the requestEnd
	 */
	public long getRequestEnd() {
		return this.requestEnd;
	}

	/**
	 * @param requestEnd
	 *            the requestEnd to set
	 */
	public void setRequestEnd(long requestEnd) {
		this.requestEnd = requestEnd;
	}

	/**
	 * @return the requestDescription
	 */
	public String getRequestDescription() {
		return this.requestDescription;
	}

	/**
	 * @param requestDescription
	 *            the requestDescription to set
	 */
	public void setRequestDescription(String requestDescription) {
		this.requestDescription = requestDescription;
	}

	/**
	 * @return the resultType
	 */
	public ResultType getResultType() {
		return this.resultType;
	}

	/**
	 * @param resultType
	 *            the resultType to set
	 */
	public void setResultType(ResultType resultType) {
		this.resultType = resultType;
	}

	/**
	 * @return the requestResult
	 */
	public List<List<String>> getRequestResult() {
		return this.requestResult;
	}

	/**
	 * @param requestResult
	 *            the requestResult to set
	 */
	public void setRequestResult(List<List<String>> requestResult) {
		this.requestResult = requestResult;
	}

}
