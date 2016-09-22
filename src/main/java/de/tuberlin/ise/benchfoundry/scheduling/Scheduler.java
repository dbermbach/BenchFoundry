/**
 * 
 */
package de.tuberlin.ise.benchfoundry.scheduling;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.tuberlin.ise.benchfoundry.util.CustomThreadFactoryBuilder;
import de.tuberlin.ise.benchfoundry.util.BenchFoundryConfigData;
import de.tuberlin.ise.benchfoundry.util.Phase;
import de.tuberlin.ise.benchfoundry.util.SelectiveLogEntry;
import de.tuberlin.ise.benchfoundry.util.Time;

/**
 * is created with a trace file as parameter. Creates Business* objects from the
 * trace and submits the {@link BusinessProcess} objects at the appropriate time
 * into a thread pool.
 * 
 * Note: Classes that instantiate {@link Scheduler} must assert that there is
 * only one single instance of {@link Scheduler} or {@link SequentialScheduler}
 * active at a time.
 * 
 * 
 * @author Dave
 *
 */
public class Scheduler implements Runnable {

	private static final Logger LOG = LogManager.getLogger(Scheduler.class);

	/**
	 * after scheduling all processes all processes are interrupted after
	 * 10*this variable in seconds
	 */
	public final static int processKilloffCounter = 1 + BenchFoundryConfigData._maximumProcessDurationInSeconds / 10;

	/** trace parser */
	private final TraceParser trace;

	private final ExecutorService pool;

	/** phase in which this scheduler is responsible */
	private final Phase phase;

	/**
	 * the amount of ms that the scheduler will use as a buffer size for reading
	 * from the trace
	 */
	private final long preBufferSize = BenchFoundryConfigData.processScheduleAheadTimeInMs
			+ BenchFoundryConfigData.transactionPrepareTimeInMs + BenchFoundryConfigData._schedulerReadBufferInMs;

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
	public Scheduler(boolean doMeasurements, String traceFilename, Phase phase) {
		super();
		trace = new TraceParser(traceFilename, doMeasurements);
		this.phase = phase;
		pool = Executors.newCachedThreadPool(new CustomThreadFactoryBuilder()
				.setNamePrefix(phase + "-thread").build());
		
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
		long prescheduleTime = BenchFoundryConfigData.processScheduleAheadTimeInMs
				+ BenchFoundryConfigData.transactionPrepareTimeInMs;
		long wait = 0;
		while (!((trace.isEndOfFile() && trace.isCacheEmpty()) || Thread
				.currentThread().isInterrupted())) {
			List<BusinessProcess> procs = trace.nextProcesses(Time.now()
					+ preBufferSize);
			if (procs.size() == 0) {
				continue;
			} else {
				// LOG.debug("Scheduling " + procs.size() +
				// " business processes.");
			}
			for (BusinessProcess proc : procs) {
				if (proc == null)
					continue;
				wait = proc.getStartTimestamp() - Time.now() - 1;
				if (SelectiveLogEntry.doDetailledLogging) {
					proc.getLog()
							.log(this,
									"Current Phase="
											+ phase
											+ ", retrieved process from log as one out of "
											+ procs.size()
											+ " processes, now waiting for "
											+ wait + "ms.");
				}
				if (wait > prescheduleTime) {
					try {
						Thread.sleep(wait - prescheduleTime - 1);
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
					}
				}
				if (SelectiveLogEntry.doDetailledLogging)
					proc.getLog().log(this, "Submitting process to pool.");
				pool.submit(proc);

			}
		}
		LOG.info("Shutdown of scheduler for phase " + phase + " initiated.");
		pool.shutdown();
		// if (Thread.currentThread().isInterrupted())
		// pool.shutdownNow();
		LOG.info("Scheduler for phase " + phase
				+ " terminated (business processes may still be running).");

		int counter = 0;
		while (!(pool.isTerminated() || Thread.currentThread().isInterrupted())) {
			try {
				pool.awaitTermination(10, TimeUnit.SECONDS);
				LOG.info("Some business processes are still running.");
				counter++;
				if (counter >= processKilloffCounter) {
					pool.shutdownNow();
					LOG.info("Forcing shutdown of pool.");
					pool.awaitTermination(10, TimeUnit.SECONDS);
					break;
				}
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}
		LOG.info("Scheduler is terminated.");
	}

}
