package de.tuberlin.ise.benchfoundry.tracegeneration.tpccinspiredbenchmark.bp;

public class Operation {

	private long opId;
	private String opStmt;
	private long paramId;
	private String paramStmt;

	public void setOpId(long opId) {
		this.opId = opId;
	}

	public void setOpStmt(String opStmt) {
		this.opStmt = opStmt;
	}

	public void setParamId(long paramId) {
		this.paramId = paramId;
	}

	public void setParamStmt(String paramStmt) {
		this.paramStmt = paramStmt;
	}

	public long getOpId() {
		return opId;
	}

	public String getOpStmt() {
		return opStmt;
	}

	public long getParamId() {
		return paramId;
	}

	public String getParamStmt() {
		return paramStmt;
	}

}
