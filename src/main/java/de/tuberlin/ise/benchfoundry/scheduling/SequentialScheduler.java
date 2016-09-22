/**
 * 
 */
package de.tuberlin.ise.benchfoundry.scheduling;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.tuberlin.ise.benchfoundry.util.BenchFoundryConfigData;
import de.tuberlin.ise.benchfoundry.util.CustomThreadFactoryBuilder;
import de.tuberlin.ise.benchfoundry.util.Phase;
import de.tuberlin.ise.benchfoundry.util.SelectiveLogEntry;

/**
 * This class should be used for preload and warm-up phases as it will ignore
 * specified start times in the trace. Instead, operations are executed
 * sequentially by a fixed number of threads.
 * 
 * 
 * is created with a trace file as parameter. Creates Business* objects from the
 * trace and submits the {@link BusinessProcess} objects at the appropriate time
 * into a thread pool.
 * 
 * Note: Classes that instantiate {@link SequentialScheduler} must assert that
 * there is only one single instance of {@link SequentialScheduler} or
 * {@link Scheduler} active at a time.
 * 
 * 
 * @author Dave
 *
 */
public class SequentialScheduler implements Runnable {

	private static final Logger LOG = LogManager
			.getLogger(SequentialScheduler.class);

	/** trace parser */
	private final TraceParser trace;

	private final static int NUMBER_OF_THREADS = BenchFoundryConfigData._numberOfThreadsInSequentialScheduler;

	private final ExecutorService pool;

	/** name of the phase in which this scheduler is responsible */
	private final Phase phase;

	/**
	 * @param doMeasurements
	 *            only log measurement results if set to true, otherwise this
	 *            business process is part of a warm up/clean action
	 * @param traceFilename
	 *            absolute path to the file which stores the trace that shall be
	 *            used
	 * @param phase
	 *            name of the phase in which this scheduler is responsible
	 */
	public SequentialScheduler(boolean doMeasurements, String traceFilename,
			Phase phase) {
		super();
		trace = new TraceParser(traceFilename, doMeasurements);
		this.phase = phase;
		pool = Executors.newFixedThreadPool(
				NUMBER_OF_THREADS,
				new CustomThreadFactoryBuilder().setNamePrefix(
						phase + "-thread").build());
		LOG.info("Scheduler for phase " + phase + " initialized.");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		LOG.info("Scheduler for phase " + phase + " started.");
		Set<Future<?>> executing = new HashSet<Future<?>>();
		Set<Future<?>> done = new HashSet<Future<?>>();
		while (!(trace.isEndOfFile() || Thread.currentThread().isInterrupted())) {
			BusinessProcess proc = trace.next();
			if (proc == null)
				continue;
			if (SelectiveLogEntry.doDetailledLogging) {
				proc.getLog()
						.log(this,
								"Current Phase="
										+ phase
										+ ", retrieved process from log, submitting to pool.");
			}
			proc.setDoTiming(false);
			Future<?> future = pool.submit(proc);
			executing.add(future);
			// if the pool is busy, check whether any of the processes has been
			// completely executed
			while (!(Thread.currentThread().isInterrupted())
					&& executing.size() >= NUMBER_OF_THREADS) {
				done.clear();
				for (Future<?> f : executing) {
					if (f.isDone())
						done.add(f);
				}
				if (done.size() == 0) {
					sleep(1);
				} else
					executing.removeAll(done);
			}
		}
		LOG.info("Shutdown of scheduler for phase " + phase + " initiated.");
		pool.shutdown();
		if (Thread.currentThread().isInterrupted())
			pool.shutdownNow();
		LOG.info("Scheduler for phase " + phase
				+ " terminated (business processes may still be running).");
		try {
			pool.awaitTermination(10, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
		}
		if (pool.isTerminated()) {
			LOG.info("All processes of phase " + phase + " terminated.");
		} else {
			LOG.info("Some processes of phase " + phase
					+ " may still be running.");
		}
	}

	/**
	 * invokes Thread.sleep() and resets the interrupted flag if an
	 * InterruptedException is caught
	 * 
	 * @param duration
	 */
	private void sleep(long duration) {
		try {
			Thread.sleep(duration);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

}
