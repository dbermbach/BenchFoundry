/**
 * 
 */
package de.tuberlin.ise.benchfoundry.scheduling;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * This class parses operation trace files
 * 
 * @author Dave
 *
 */
public class TraceParser {

	/** reads the input file */
	private BufferedReader input;

	/** if true the end of the stream has been reached */
	private boolean endOfFile = false;

	/** is passed on all created {@link BusinessProcess} instances */
	private boolean doMeasurements;

	/** entries that have been read from the trace but not yet returned */
	private List<BusinessProcess> cache = new ArrayList<>();

	/**
	 * 
	 * @param traceFile
	 *            the location of the trace file that shall be parsed
	 * @param doMeasurements
	 *            describes whether measurement results shall be persisted and
	 *            is passed on all created {@link BusinessProcess} instances
	 */
	public TraceParser(String traceFile, boolean doMeasurements) {
		this.doMeasurements = doMeasurements;
		try {
			input = new BufferedReader(new FileReader(traceFile));
		} catch (NullPointerException | FileNotFoundException e) {
			endOfFile = true;
		}
	}

	/**
	 * 
	 * @param traceStream
	 *            a stream from the trace file that shall be parsed
	 * @param doMeasurements
	 *            describes whether measurement results shall be persisted and
	 *            is passed on all created {@link BusinessProcess} instances
	 */
	public TraceParser(InputStream traceStream, boolean doMeasurements) {
		this.doMeasurements = doMeasurements;
		input = new BufferedReader(new InputStreamReader(traceStream));
	}

	/**
	 * Create a trace parser from an open input reader.
	 * 
	 * @param reader
	 * @param doMeasurements
	 */
	public TraceParser(BufferedReader reader, boolean doMeasurements) {
		this.doMeasurements = doMeasurements;
		if ((input = reader) == null) {
			endOfFile = true;
		}
	}

	/**
	 * Please, note: either use nextProcesses or next but not a mixture of both.<br>
	 * 
	 * 
	 * @param beforeTimestamp
	 * @return a list of all process entries that are to be started before the
	 *         specified relative timestamp and that have not been returned yet.
	 *         List may be empty but will not be null.
	 */
	public List<BusinessProcess> nextProcesses(long beforeTimestamp) {
		List<BusinessProcess> result = new ArrayList<>();
		BusinessProcess temp;
		while (cache.size() > 0) {
			temp = cache.get(0);
			if (temp.getStartTimestamp() <= beforeTimestamp) {
				result.add(temp);
				cache.remove(0);
			} else
				break;
		}
		temp = this.next();
		if (temp == null)
			return result; // we got all & end of trace
		while (temp.getStartTimestamp() <= beforeTimestamp) {
			result.add(temp);
			temp = this.next();
			if (temp == null)
				return result; // we got all & end of trace
		}
		cache.add(temp);
		return result;
	}

	/**
	 * Please, note: Optional param or cust param ids will be set as -1 if not
	 * present <br>
	 * 
	 * Please, note: either use nextProcesses or next but not a mixture of both.<br>
	 * 
	 * @return the next {@link BusinessProcess} instance from the trace or null
	 *         if an error occurred. Use isEndOfFile() to query whether there
	 *         are more entries. <br>
	 *         FIXME will always return a final null value before isEndOfFile
	 *         returns true
	 */
	public BusinessProcess next() {
		if (endOfFile)
			return null;
		ArrayList<String> lines = new ArrayList<>();
		try {
			String line;

			while ((line = input.readLine()) != null) {
				if (line.startsWith("#") || line.length() == 0
						|| line.matches("\\s"))
					continue; // comment line
				lines.add(line);
				if (line.startsWith("EOP")) {
					// found the next process entry
					break;
				}
			}
			endOfFile = line == null;
			if (lines.size() < 5) {
				// BOP-BOT-op-EOT-EOP is minimal form
				return null;
			}
		} catch (IOException io) {
			io.printStackTrace();
			return null;
		}
		// assemble objects
		String[] splits;
		List<BusinessOperation> ops = new ArrayList<>();
		List<BusinessTransaction> txs = new ArrayList<>();
		long processId = 0, processTimestamp = 0, txId = 0, txDelay = 0, opId = 0;
		for (String line : lines) {
			splits = line.split(";");
			switch (splits[0]) {
			case "BOP":
				processTimestamp = Long.parseLong(splits[1].trim());
				processId = Long.parseLong(splits[2].trim());
				break;
			case "EOP":
				return new BusinessProcess(processId, txs, processTimestamp,
						doMeasurements);
			case "BOT":
				txDelay = Long.parseLong(splits[1].trim());
				break;
			case "EOT":
				txs.add(new BusinessTransaction(txId++, ops, txDelay));
				ops.clear();
				opId = 0;
				break;
			default:
				if (splits.length == 3)
					ops.add(new BusinessOperation(opId++, Integer
							.parseInt(splits[0].trim()), Integer
							.parseInt(splits[1].trim()), Integer
							.parseInt(splits[2].trim())));
				else if (splits.length == 2)
					ops.add(new BusinessOperation(opId++, Integer
							.parseInt(splits[0].trim()), Integer
							.parseInt(splits[1].trim()), -1));
				else
					ops.add(new BusinessOperation(opId++, Integer
							.parseInt(splits[0].trim()), -1, -1));

				break;
			}
		}
		return null;
	}

	/**
	 * @return whether the end of the trace file has been reached
	 */
	public boolean isEndOfFile() {
		return this.endOfFile;
	}

	/**
	 * 
	 * @return whether there are still some cached trace entries that have not
	 *         been returned yet
	 */
	public boolean isCacheEmpty() {
		return cache.isEmpty();
	}

}
