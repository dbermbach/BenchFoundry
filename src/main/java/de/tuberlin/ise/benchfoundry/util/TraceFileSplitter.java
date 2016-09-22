package de.tuberlin.ise.benchfoundry.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.tuberlin.ise.benchfoundry.scheduling.BusinessProcess;
import de.tuberlin.ise.benchfoundry.scheduling.TraceParser;

/**
 * @author Akon Dey (akon.dey@sydney.edu.au)
 *
 */
public class TraceFileSplitter {
	private static final Logger LOG = LogManager
			.getLogger(TraceFileSplitter.class);

	/**
	 * Spray the business processes logged in the input trace file to multiple
	 * output trace files in Round Robin fashion.
	 * 
	 * @param is
	 *            trace file input stream
	 * @param os
	 *            array of output streams where business processes are written
	 *            to
	 *
	 * @throws IOException
	 */
	public static void sprayTraceInputStreamToOutputStreams(InputStream is,
			List<OutputStream> os) throws IOException {
		TraceParser trace = new TraceParser(new BufferedReader(
				new InputStreamReader(is)), true);
		LOG.debug("Spraying process definitions started.");

		List<BufferedWriter> ow = new ArrayList<BufferedWriter>(os.size());
		for (OutputStream o : os) {
			ow.add(new BufferedWriter(new OutputStreamWriter(o)));
		}

		for (int i = 0; !(trace.isEndOfFile() || Thread.currentThread()
				.isInterrupted()); i = (i + 1) % os.size()) {
			BusinessProcess proc = trace.next();
			if (proc == null)
				continue;
			try {
				ow.get(i).write(proc.toString() + "\n");
			} catch (IOException e) {
				LOG.error("Failed to write to OutputStream: " + os.get(i));
				throw e;
			}
		}

		for (BufferedWriter o : ow) {
			try {
				o.flush();
				o.close();
			} catch (IOException e) {
				LOG.error("Failed to flush BufferedWriter: " + o);
				throw e;
			}
		}
		is.close();

		LOG.debug("Spraying process definitions completed.");
	}

	/**
	 * Spray the business processes logged in the input trace file to multiple
	 * output trace files in Round Robin fashion.
	 * 
	 * 
	 * @param inputFile
	 *            name of the file where the full trace resides
	 * @param outputFiles
	 *            names of output files where a partial trace shall be written
	 * @throws IOException
	 */
	public static void splitTrace(String inputFile, List<String> outputFiles)
			throws IOException {
		List<OutputStream> out = new ArrayList<>();
		for (String of : outputFiles)
			out.add(new FileOutputStream(of));
		if (inputFile == null || !new File(inputFile).isFile()) {
			LOG.info("Trace file is null, creating empty trace files.");
			for (OutputStream os : out) {
				os.write("# empty file".getBytes());
				os.close();
			}
		} else
			sprayTraceInputStreamToOutputStreams(
					new FileInputStream(inputFile), out);

	}

}
