package de.tuberlin.ise.benchfoundry.tracegeneration.tpccinspiredbenchmark.bp;

import java.util.ArrayList;
import java.util.List;

public class Process {

	private long id;
	private long start;
	private final List<Transaction> transactions = new ArrayList<>();

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public long getStart() {
		return start;
	}

	public void setStart(long start) {
		this.start = start;
	}

	public void addTransaction(Transaction t) {
		transactions.add(t);
	}
	
	public List<Transaction> getTransactions() {
		return transactions;
	}

}
