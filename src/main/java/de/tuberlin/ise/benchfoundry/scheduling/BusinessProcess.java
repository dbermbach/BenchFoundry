/**
 * 
 */
package de.tuberlin.ise.benchfoundry.scheduling;

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.tuberlin.ise.benchfoundry.util.BenchFoundryConfigData;
import de.tuberlin.ise.benchfoundry.util.MicroStatisticsCollector;
import de.tuberlin.ise.benchfoundry.util.Phase;
import de.tuberlin.ise.benchfoundry.util.PhaseManager;
import de.tuberlin.ise.benchfoundry.util.SelectiveLogEntry;
import de.tuberlin.ise.benchfoundry.util.Time;

/**
 * describes a sequence of transactions ({@link BusinessTransaction}). This
 * forms the unit of distribution to different machines within a BenchFoundry cluster
 * 
 * @author Dave
 *
 */
public class BusinessProcess implements Runnable {

	private static final Logger LOG = LogManager
			.getLogger(BusinessProcess.class);

	/** the ID of this business process */
	private final long id;

	/** holds all operations within the scope of this business transaction */
	private final List<BusinessTransaction> transactions = new ArrayList<BusinessTransaction>();

	/**
	 * the number of milliseconds between the overall benchmark start and the
	 * execution start of this process
	 */
	private final long startTimestamp;

	/**
	 * only log measurement results if set to true, otherwise this business
	 * process is part of a warm up/clean action
	 */
	private boolean doMeasurements;

	/**
	 * if false, all timestamps will be ignored instead executing this process
	 * right away and as fast as possible
	 */
	private boolean doTiming = true;

	/**
	 * collect detailed information in here during execution, print if any
	 * issues are encountered
	 */
	private final SelectiveLogEntry log = new SelectiveLogEntry();

	/**
	 * @param id
	 *            the ID of this business process
	 * @param transactions
	 *            all operations within the scope of this business transaction
	 * @param startTimestamp
	 *            the number of milliseconds between the overall benchmark start
	 *            and the execution start of this process
	 * @param doMeasurements
	 *            measurement results shall only be logged if this is true.
	 *            Otherwise the process is part of a clean-up/warm-up activity
	 */
	BusinessProcess(long id, List<BusinessTransaction> transactions,
			long startTimestamp, boolean doMeasurements) {
		super();
		this.id = id;
		this.transactions.addAll(transactions);
		this.startTimestamp = startTimestamp;
		this.doMeasurements = doMeasurements;
		for (BusinessTransaction bt : transactions)
			bt.setSurroundingProcess(this);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		if (PhaseManager.getInstance().getCurrentPhase() == Phase.EXPERIMENT)
			this.doMeasurements = true;
		byte state = 0;
		// LOG.debug("process pId=" + id + " started.");
		boolean doLog = SelectiveLogEntry.doDetailledLogging;
		if (doLog) {
			log.log(this, "Execution started for process pId=" + id);
			log.logLocalVariable("startTimestamp", startTimestamp);
			log.logLocalVariable("doMeasurements", doMeasurements);
			log.logLocalVariable("doTiming", doTiming);
		}
		try {
			if (doTiming) {
				// only if run by Scheduler (not: SequentialScheduler)
				Time.waitUntilRelativeTime(startTimestamp
						- BenchFoundryConfigData.transactionPrepareTimeInMs);
				state = 1;
			}
			if (doLog)
				log.log(this, "Preparing transactions.");
			// LOG.debug("process pId=" + id + ": preparing transactions.");
			for (BusinessTransaction bt : transactions) {
				bt.prepare(doMeasurements, log);
			}
			state = 2;

			// LOG.debug("process pId=" + id + ": " + transactions.size()
			// + " transactions have been prepared.");
			if (doLog)
				log.log(this, transactions.size()
						+ " transactions have been prepared.");
			if (doTiming) {
				// only if run by Scheduler (not: SequentialScheduler)
				Time.waitUntilRelativeTime(startTimestamp);
				state = 3;
			}
			if (doLog)
				log.log(this, "Starting execution (supposed start="
						+ startTimestamp + ").");
			// LOG.debug("execution of process pId=" + id
			// + " started; (supposed start=" + startTimestamp + ", actual="
			// + Time.now() + ")");

			long now = 0;
			if (doMeasurements)
				now = Time.now();
			for (BusinessTransaction bt : transactions) {
				bt.execute(log);
			}
			state = 4;
			if (doMeasurements)
				MicroStatisticsCollector.addSchedulingDelay(now
						- startTimestamp);

			// LOG.debug("process pId=" + id + " completed.");
			if (doLog)
				log.log(this, "Execution completed.");
		} catch (InterruptedException ie) {
			String info = "Process " + id
					+ " could not complete because it was interrupted ";
			switch (state) {
			case 0:
				info += "before it could actually start.";
				break;
			case 1:
				info += "while preparing transactions.";
				break;
			case 2:
				info += "while waiting to start executing.";
				break;
			case 3:
				info += "while executing transactions.";
				break;
			default:
				info += "after executing all transactions.";
				break;
			}
			LOG.info(info);
		} catch (Exception t) {
			if (PhaseManager.getInstance().getCurrentPhase() == Phase.CLEANUP
					|| PhaseManager.getInstance().getCurrentPhase() == Phase.TERMINATED) {
				String info = "Current phase is CLEANUP or TERMINATED, process "
						+ id
						+ " could not complete because it was interrupted ";
				switch (state) {
				case 0:
					info += "before it could actually start.";
					break;
				case 1:
					info += "while preparing transactions.";
					break;
				case 2:
					info += "while waiting to start executing.";
					break;
				case 3:
					info += "while executing transactions.";
					break;
				default:
					info += "after executing all transactions.";
					break;
				}
				LOG.info(info);
			}
			if (doLog) {
				log.log(this, "An error occured", t);
				LOG.error("An error occured, could not complete process pid="
						+ id, t);
				LOG.debug("An error occured, could not complete process pid="
						+ id + "+. Details:" + log);
			} else {
				LOG.error("An error occured, could not complete process pid="
						+ id, t);
				t.printStackTrace();
			}
		}
	}

	/**
	 * @return the id
	 */
	public long getId() {
		return this.id;
	}

	/**
	 * @return the transactions
	 */
	public List<BusinessTransaction> getTransactions() {
		return this.transactions;
	}

	/**
	 * @return the startTimestamp
	 */
	public long getStartTimestamp() {
		return this.startTimestamp;
	}

	/**
	 * @return the doMeasurements
	 */
	public boolean isDoMeasurements() {
		return this.doMeasurements;
	}

	/**
	 * @return the doTiming
	 */
	public boolean isDoTiming() {
		return this.doTiming;
	}

	/**
	 * if set to false, this process will run right away and will ignore all
	 * start times
	 * 
	 * @param doTiming
	 *            the doTiming to set
	 */
	public void setDoTiming(boolean doTiming) {
		this.doTiming = doTiming;
	}

	public String toString() {
		String str = new String();
		str += "BOP;" + this.startTimestamp + ";" + this.id + "\n";
		for (BusinessTransaction t : this.transactions) {
			str += t.toString() + "\n";
		}
		str += "EOP\n";
		return str;
	}

	/**
	 * @return the log
	 */
	public SelectiveLogEntry getLog() {
		return this.log;
	}

}
