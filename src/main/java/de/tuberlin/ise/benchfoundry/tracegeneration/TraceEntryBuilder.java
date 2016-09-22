/**
 * 
 */
package de.tuberlin.ise.benchfoundry.tracegeneration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * builder class which can be used in trace generators; automatically handles
 * deduplication of params etc.; business processes can be created through
 * method chaining.
 * 
 * @author Dave
 *
 */
public class TraceEntryBuilder {

	private long nextProcessId = 0;
	private long nextQueryId = 0;
	private long nextParamId = 0;
	private long nextCustParamId = 0;

	private Map<String, Long> paramsReg = new HashMap<>();
	private Map<String, Long> custParamsReg = new HashMap<>();
	private Map<String, Long> queryReg = new HashMap<>();

	/**
	 * starts building a new process. Invoke build() in the end
	 * 
	 * @param startTimestamp
	 *            relative start timestamp of the business process in ms
	 * @return
	 */
	public BProcess newProcess(long startTimestamp) {
		BProcess bp = new BProcess();
		bp.startTimestamp = startTimestamp;
		return bp;
	}

	/**
	 * builds the parameter input file based on all parameters that have been
	 * registered through addOp() invocations
	 * 
	 * @return a string that can directly be written to file
	 */
	public String buildParamsFile() {
		StringBuffer sb = new StringBuffer(
				"# BenchFoundry parameter input file");
		for (Entry<String, Long> entry : paramsReg.entrySet()) {
			sb.append("\n" + entry.getValue() + ":" + entry.getKey());
		}
		return sb.toString();
	}

	/**
	 * builds the custom parameter input file based on all custom parameters
	 * that have been registered through addOp() invocations
	 * 
	 * @return a string that can directly be written to file
	 */
	public String buildCustParamsFile() {
		StringBuffer sb = new StringBuffer(
				"# BenchFoundry custom parameter input file");
		for (Entry<String, Long> entry : custParamsReg.entrySet()) {
			sb.append("\n" + entry.getValue() + ":" + entry.getKey());
		}
		return sb.toString();
	}

	/**
	 * builds the query/operation list input file based on all queries that have
	 * been used in addOp() invocations
	 * 
	 * @return a string that can directly be written to file
	 */
	public String buildQueryList() {
		StringBuffer sb = new StringBuffer("# BenchFoundry query list");
		for (Entry<String, Long> entry : queryReg.entrySet()) {
			sb.append("\n" + entry.getValue() + ":" + entry.getKey());
		}
		return sb.toString();
	}

	public class BProcess {
		List<BTransaction> txs = new ArrayList<>();
		long startTimestamp;

		BTransaction current;

		/**
		 * always call a sequence of bot() [addop()]+ eot()
		 * 
		 * @param startDelay
		 *            an additional delay before the start of the transaction
		 *            (in ms)
		 * @return
		 */
		public BProcess bot(long startDelay) {
			if (current != null)
				throw new RuntimeException(
						"Call eot() before invoking bot() again.");
			current = new BTransaction();
			current.startDelay = startDelay;
			txs.add(current);
			return this;
		}

		/**
		 * ends the current transaction
		 * 
		 * @return
		 */
		public BProcess eot() {
			current = null;
			return this;
		}

		private BProcess addOp(long queryId, long paramId, long custParamId) {
			current.addOp(queryId, paramId, custParamId);
			return this;
		}

		/**
		 * adds an operation to the active transaction. Registers the specified
		 * query and params if not done yet or retrieves their ids.
		 * 
		 * @param query
		 *            sql query string
		 * @param params
		 *            a string array of individual parameters
		 * @param custParams
		 *            a string array of individual parameters
		 * @return
		 */
		public BProcess addOp(String query, String[] params, String[] custParams) {
			if (query == null || params == null)
				throw new RuntimeException("query and params may not be null!");
			// build entry for param file
			StringBuffer ps = new StringBuffer();
			for (String s : params)
				ps.append(";" + s);
			ps.deleteCharAt(0);
			// get id for param file entry or register it for new id
			Long pId = paramsReg.get(ps.toString());
			if (pId == null) {
				pId = nextParamId++;
				paramsReg.put(ps.toString(), pId);
			}
			// build entry for cust param file if there is one
			Long cpId;
			if (custParams == null || custParams.length == 0)
				cpId = -1L;
			else {
				StringBuffer cps = new StringBuffer();
				for (String s : custParams)
					cps.append(";" + s);
				cps.deleteCharAt(0);
				// get id for cust param file entry or register it for new id
				cpId = custParamsReg.get(cps.toString());
				if (cpId == null) {
					cpId = nextCustParamId++;
					custParamsReg.put(cps.toString(), cpId);
				}
			}
			// get id for query/oplist entry or register it for new id
			Long qId = queryReg.get(query);
			if (qId == null) {
				qId = nextQueryId++;
				queryReg.put(query, qId);
			}

			return addOp(qId, pId, cpId);
		}

		/**
		 * adds an operation to the active transaction. Registers the specified
		 * query and params if not done yet or retrieves their ids.
		 * 
		 * @param query
		 *            sql query string
		 * @param params
		 *            a string array of individual parameters
		 * @return
		 */
		public BProcess addOp(String query, String... params) {
			return addOp(query, params, null);

		}

		/**
		 * 
		 * @return a string representation ready for writing to the trace file
		 */
		public String build() {
			StringBuffer build = new StringBuffer("BOP;" + startTimestamp + ";"
					+ nextProcessId++);
			for (BTransaction tx : txs) {
				build.append("\n" + tx.build());
			}
			build.append("\nEOP");
			return build.toString();
		}

	}

	private class BTransaction {
		private List<BOperation> bops = new ArrayList<>();
		private long startDelay = 0;

		private void addOp(long opId, long paramId, long custParamId) {
			bops.add(new BOperation(opId, paramId, custParamId));
		}

		private String build() {
			StringBuffer build = new StringBuffer("BOT;" + startDelay);
			for (BOperation bo : bops) {
				build.append("\n" + bo.build());
			}
			build.append("\nEOT");
			return build.toString();
		}
	}

	private class BOperation {
		private long opId;
		private long paramId;
		private long custParamId = -1;

		public BOperation(long opId, long paramId, long custParamId) {
			super();
			this.opId = opId;
			this.paramId = paramId;
			this.custParamId = custParamId;
		}

		private String build() {
			return opId + ";" + paramId + ";" + custParamId;
		}
	}

}
