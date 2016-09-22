package de.tuberlin.ise.benchfoundry.connectors;

import java.util.ArrayList;
import java.util.List;

import de.tuberlin.ise.benchfoundry.connectors.exceptions.PrepareTransactionException;
import de.tuberlin.ise.benchfoundry.physicalschema.model.AbstractRequest;
import de.tuberlin.ise.benchfoundry.physicalschema.model.relational.RelationalRequest;
import de.tuberlin.ise.benchfoundry.util.BenchFoundryConfigData;
import de.tuberlin.ise.benchfoundry.util.SelectiveLogEntry;

/**
 * 
 * @author joernkuhlenkamp
 *
 */
public abstract class RelationalDbConnector implements IDbConnector {

	@Override
	public void prepareTransaction(long processId, long transactionId,
			List<List<? extends AbstractRequest>> operations,
			List<Integer> businessOperationIds, List<List<String>> params,
			List<List<String>> custParams, boolean doMeasurements,
			SelectiveLogEntry log) throws PrepareTransactionException {

		List<RelationalRequest> ops = new ArrayList<>();
		for (List<? extends AbstractRequest> l : operations) {
			if (l.size() > 1)
				throw new PrepareTransactionException(
						"More than one request in operation: " + l);
			for (AbstractRequest r : l) {
				ops.add((RelationalRequest) r);
			}
		}
		if (SelectiveLogEntry.doDetailledLogging)
			log.log(RelationalDbConnector.class, "All requests have been typecast.");
		prepareRelationalTransaction(processId, transactionId, ops,
				businessOperationIds, params, doMeasurements, log);
	}

	protected abstract void prepareRelationalTransaction(long processId,
			long transactionId, List<RelationalRequest> operations,
			List<Integer> businessOperationIds, List<List<String>> params,
			boolean doMeasurements, SelectiveLogEntry log)
			throws PrepareTransactionException;

	protected void logTransactionResult() {

	}
	
	/**
	 * replaces all "?" wildcards with the paramters from "params" in the order
	 * specified in that method.
	 * 
	 * @param req
	 *            a SQL String with "?" wildcards for parameter values
	 * @param params
	 *            a sequence of parameters
	 * @param log
	 * @return a SQL String without wildcards
	 * @throws PrepareTransactionException
	 *             if the number of wildcards in req does not equal the number
	 *             of entries in params
	 */
	protected String reassembleSQLRequest(String req, List<String> params,
			SelectiveLogEntry log) throws PrepareTransactionException {
		// Determine number of "?" occurrences in SQL string of {@link
		// BusinessTransaction}

		int paramNumb = 0;
		for (char c : req.toCharArray())
			if (c == '?')
				paramNumb++;
		// LOG.debug("req=" + req + ", params=" + params + ", paramNumb="
		// + paramNumb);
		if (SelectiveLogEntry.doDetailledLogging) {
			log.logLocalVariable("req", req);
			log.logLocalVariable("params", params);
			log.logLocalVariable("paramNumb", paramNumb);
		}
		if (paramNumb != params.size()) {
			if (SelectiveLogEntry.doDetailledLogging) {
				log.log(this, "Have " + params.size() + " params, should be "
						+ paramNumb + ":");
				for (String s : params)
					log.log(this, "param entry:" + s);
			}
			throw new PrepareTransactionException("Have " + params.size()
					+ " params, should be " + paramNumb + "(" + params + ")");
		}

		StringBuilder sb = new StringBuilder(req);
		for (int i = 0; i < paramNumb; i++) {
			int index = sb.indexOf("?");
			sb.replace(index, index + 1, params.get(i));
			// log.logLocalVariable("sb", sb);
		}
		return sb.toString();
	}
}
