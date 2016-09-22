/**
 * 
 */
package de.tuberlin.ise.benchfoundry.util;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * utility class for calculating relative timestamps. Default value for
 * benchmark start time is when the classloader loads that class.
 * 
 * @author Dave
 *
 */
public class Time {

	private static final Logger LOG = LogManager.getLogger(Time.class);

	private static final SimpleDateFormat sdf = new SimpleDateFormat(
			"HH':'mm':'ss','SSS");
	private static final Date cachedSdfDate = new Date();

	/**
	 * agreed upon time for the benchmark start; is used to calculate relative
	 * timestamps
	 */
	private static Long benchmarkStartTime = null;

	/** if true the start time will never again change. */
	private static boolean startTimeSet = false;

	/**
	 * @return the benchmarkStartTime which is used to calculate relative
	 *         timestamps. Will be null if not set yet.
	 */
	public static Long getBenchmarkStartTime() {
		return benchmarkStartTime;
	}

	/**
	 * sets the benchmark start time that is used to calculate relative
	 * timestamps
	 * 
	 * @param benchmarkStartTime
	 *            the benchmarkStartTime to set
	 */
	public static void setBenchmarkStartTime(long benchmarkStartTime) {
		if (startTimeSet)
			throw new RuntimeException(
					"The start time has already been set and cannot be overwritten.");
		Time.benchmarkStartTime = benchmarkStartTime;
		startTimeSet = true;
		LOG.info("Start time for experiment phase has been set to "
				+ format(benchmarkStartTime));
		if (BenchFoundryConfigData.doDetailledLoggingForExceptions)
			SelectiveLogEntry.doDetailledLogging = true;
	}

	/**
	 * 
	 * @return the elapsed time relative to the benchmark start time stamp
	 * @throws RuntimeException
	 *             if the benchmark start time has not been set yet.
	 */
	public static long now() {
		if (benchmarkStartTime == null)
			throw new RuntimeException(
					"Benchmark start time must be set explicitly before"
							+ " relative start times can be calculated.");
		return System.currentTimeMillis() - benchmarkStartTime;
	}

	/**
	 * waits until the specified relative timestamp (with the benchmark start
	 * time as relative zero)
	 * 
	 * @param time
	 * @throws InterruptedException
	 *             if the invoking thread is interrupted
	 * @throws RuntimeException
	 *             if the benchmark start time has not been set yet.
	 */
	public static void waitUntilRelativeTime(long time)
			throws InterruptedException {
		while (now() + 10 < time)
			Thread.sleep(time - now() - 5);
		while (now() + 1 < time)
			Thread.sleep(1);
		while (now() < time)
			Thread.yield();
	}

	/**
	 * The start time will never again change once it has been set across the
	 * cluster.
	 * 
	 * @return the startTimeSet
	 */
	public static boolean isStartTimeSet() {
		return startTimeSet;
	}

	/**
	 * 
	 * @param absoluteTimestamp
	 * @return a printable form for the specified timestamp comprising hours,
	 *         minutes, seconds, ms.
	 */
	public static String format(long absoluteTimestamp) {
		synchronized (cachedSdfDate) {
			cachedSdfDate.setTime(absoluteTimestamp);
			return sdf.format(cachedSdfDate);
		}

	}

	/**
	 * 
	 * 
	 * @param durationInMs
	 * @return a formatted String describing a duration in the format
	 *         HH:MM:SS,mmm
	 * 
	 *         for values larger than 99 hours additional Hs will be added.
	 */
	public static String formatDuration(long durationInMs) {
		StringBuilder sb = new StringBuilder(12);
		long temp = durationInMs / (1000 * 60 * 60);
		if (temp < 10)
			sb.append("0" + temp + ":");
		else
			sb.append(temp + ":");
		durationInMs %= (1000 * 60 * 60);

		temp = durationInMs / (1000 * 60);
		if (temp < 10)
			sb.append("0" + temp + ":");
		else
			sb.append(temp + ":");
		durationInMs %= (1000 * 60);

		temp = durationInMs / 1000;
		if (temp < 10)
			sb.append("0" + temp);
		else
			sb.append(temp);
		temp = durationInMs % 1000;

		if (temp < 10)
			sb.append(",00" + temp);
		else if (temp < 100)
			sb.append(",0" + temp);
		else
			sb.append("," + temp);

		return sb.toString();
	}

}
