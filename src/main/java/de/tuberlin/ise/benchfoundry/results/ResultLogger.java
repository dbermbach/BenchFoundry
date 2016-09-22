/**
 * 
 */
package de.tuberlin.ise.benchfoundry.results;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.tuberlin.ise.benchfoundry.util.BenchFoundryConfigData;
import de.tuberlin.ise.benchfoundry.util.Phase;
import de.tuberlin.ise.benchfoundry.util.PhaseChangeListener;
import de.tuberlin.ise.benchfoundry.util.PhaseManager;
import de.tuberlin.ise.benchfoundry.util.Time;

/**
 * persists all {@link BusinessOperationResult} objects received via its input
 * queue
 * 
 * @author Dave
 *
 */
public class ResultLogger implements Runnable, PhaseChangeListener {

	private final ConcurrentLinkedQueue<BusinessOperationResult> inputQueue = new ConcurrentLinkedQueue<BusinessOperationResult>();

	private static final Logger LOG = LogManager.getLogger(ResultLogger.class);
	private static final String RESULT_LOG = BenchFoundryConfigData.name+"-result.log";

	
	
	/** singleton object */
	private static ResultLogger instance;

	/** indicates whether this process should be running */
	private volatile boolean running = true;

	// private BufferedWriter writer;

	private ResultLogger() throws IOException {
		// this.out = Paths.get(BenchFoundryConfigData.resultDir);
		File outputDir = new File(BenchFoundryConfigData.resultDir);
		if (!outputDir.isDirectory())
			if (outputDir.mkdir()) {
				LOG.info("Output directory "
						+ BenchFoundryConfigData.resultDir
						+ " created.");
			} else
				throw new RuntimeException("Could not create output directory "
						+ BenchFoundryConfigData.resultDir);

		
	}

	public static synchronized ResultLogger getInstance() {
		if (ResultLogger.instance == null) {
			try {
				ResultLogger.instance = new ResultLogger();
				PhaseManager.getInstance().registerListener(instance);
			} catch (IOException e) {
				LOG.error("Cannot initialize result logger. Exiting...");
				e.printStackTrace();
				System.exit(1);
			}
		}
		return ResultLogger.instance;
	}

	private String getFileName() throws UnknownHostException {
		String filename = "";
		String filenameDelimiter = "-";
		if (Time.isStartTimeSet()) {
			filename += Time.getBenchmarkStartTime();
			filename += filenameDelimiter;
		}
		filename += InetAddress.getLocalHost().getHostAddress();
		return filename;
	}

	private static void isValidPath(Path p) throws IOException {
		if (Files.notExists(p) && Files.isWritable(p)) {
			throw new IOException();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		LOG.info("ResultLogger started.");
		String s;
		try (PrintWriter writer = new PrintWriter(
				BenchFoundryConfigData.resultDir + "/" + RESULT_LOG)) {
			while (running && !Thread.currentThread().isInterrupted()) {
				while (!inputQueue.isEmpty()) {
					s = inputQueue.poll().toCsv();
								writer.println(s);
				}
				writer.flush();
				try {
					Thread.sleep(BenchFoundryConfigData._resultLoggerPollIntervalInMs);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
			}
			while (!inputQueue.isEmpty()) {
				s = inputQueue.poll().toCsv();
			
				writer.println(s);
			}
			writer.close();
		} catch (IOException e) {
			// FIXME do we want graceful degradation in the presence of IO
			// failures?
			LOG.error("Could not write output: "+e,e);
			e.printStackTrace();
		}
		LOG.info("ResultLogger terminated.");
	}

	/**
	 * marks the specified result object as pending for persistence
	 * 
	 * @param result
	 */
	public void persist(BusinessOperationResult result) {
		inputQueue.add(result);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * de.tuberlin.ise.benchfoundry.util.PhaseChangeListener#onPhaseChange(de.tuberlin
	 * .ise.benchfoundry.util.Phase, de.tuberlin.ise.benchfoundry.util.Phase)
	 */
	@Override
	public void onPhaseChange(Phase oldPhase, Phase newPhase) {
		if (oldPhase == Phase.WARMUP && newPhase == Phase.EXPERIMENT) {
			Thread t = new Thread(this);
			t.setName("LOG");
			t.start();
		} else if (newPhase == Phase.TERMINATED) {
			running = false;
		}

	}

}