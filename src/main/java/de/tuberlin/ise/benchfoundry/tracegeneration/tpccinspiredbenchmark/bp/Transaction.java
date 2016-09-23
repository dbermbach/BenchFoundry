package de.tuberlin.ise.benchfoundry.tracegeneration.tpccinspiredbenchmark.bp;

import java.util.ArrayList;
import java.util.List;

public class Transaction {

	private long id;
	private long start;
	private final List<Operation> ops = new ArrayList<>();

	public long getId() {
		return id;
	}

	public long getStart() {
		return start;
	}

	public List<Operation> getOps() {
		return ops;
	}
	
	public void setId(long id) {
		this.id = id;
	}

	public void setStart(long start) {
		this.start = start;
	}

	public void addOp(Operation operation) {
		ops.add(operation);
	}
}
