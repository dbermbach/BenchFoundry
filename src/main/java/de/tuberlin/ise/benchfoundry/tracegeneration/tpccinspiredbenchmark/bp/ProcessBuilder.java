package de.tuberlin.ise.benchfoundry.tracegeneration.tpccinspiredbenchmark.bp;

import java.io.IOException;

import de.tuberlin.ise.benchfoundry.tracegeneration.tpccinspiredbenchmark.TraceFileWriter;
import de.tuberlin.ise.benchfoundry.tracegeneration.tpccinspiredbenchmark.TraceFileWriter.TraceFile;


public class ProcessBuilder {

	private Process p;
	private Transaction t;
	private Operation o;
	private long pCount = 0;
	private long tCount = 0;
	private final TraceFileWriter writer;
	
	public ProcessBuilder(TraceFileWriter writer) {
		this.writer = writer;
	}
	
	public ProcessBuilder p(long start) {
		p = new Process();
		p.setId(pCount);
		p.setStart(start);
		tCount = 0;
		t = null;
		o = null;
		pCount++;
		return this;
	}
	
	public ProcessBuilder t(long start) {
		t = new Transaction();
		t.setId(tCount);
		t.setStart(start);
		p.addTransaction(t);
		o = null;
		tCount++;
		return this;
	}
	
	public ProcessBuilder o(String opStmt, String paramStmt) {
		o = new Operation();
		o.setOpStmt(opStmt);
		o.setParamStmt(paramStmt);
		t.addOp(o);
		return this;
	}
	
	public ProcessBuilder tId(long id) {
		t.setId(id);
		return this;
	}
	
	public ProcessBuilder tStart(long start) {
		t.setStart(start);
		return this;
	}
	
	public ProcessBuilder opId(long id) {
		o.setOpId(id);
		return this;
	}
	
	public ProcessBuilder opStmt(String stmt) {
		o.setOpStmt(stmt);
		return this;
	}
	
	public ProcessBuilder paId(long id) {
		o.setParamId(id);
		return this;
	}
	
	public ProcessBuilder paStmt(String stmt) {
		o.setOpStmt(stmt);
		return this;
	}
	
	/**
	 * 
	 * @param phase Only LOAD, WARM and RUN are permitted
	 * @return
	 * @throws IOException 
	 */
	public Process build(TraceFile file) throws IOException {
		// TODO
		String processString = "BOP;"+p.getStart()+";"+p.getId()+"\n";
		for(Transaction transaction : p.getTransactions()) {
			processString += "BOT;"+transaction.getStart()+"\n";
			for(Operation operation : transaction.getOps()) {
				operation.setOpId(writer.append(TraceFile.OPERATION, operation.getOpStmt()));
				operation.setParamId(writer.append(TraceFile.PARAM, operation.getParamStmt()));
				processString += operation.getOpId()+";"+operation.getParamId()+"\n";
			}
			processString += "EOT\n";
		}
		processString += "EOP\n";
		writer.append(file, processString);
		return p;
	}
}
