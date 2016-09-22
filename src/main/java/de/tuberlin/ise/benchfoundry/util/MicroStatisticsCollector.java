/**
 * 
 */
package de.tuberlin.ise.benchfoundry.util;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * @author Dave
 *
 */
public class MicroStatisticsCollector {

	private static final Logger LOG = LogManager.getLogger();

	private static final List<Short> schedulingDelays = new ArrayList<>();

	/**
	 * collects micro statistics on scheduling precision
	 * 
	 * @param millis
	 */
	public static void addSchedulingDelay(long millis) {
		// LOG.debug("received a scheduling delay: "+millis);
		schedulingDelays.add((short) millis);
	}

	/**
	 * prints scheduling delay aggregates
	 */
	public static void printSchedulingDelayAggregates() {
		double avg = 0, absAvg = 0, maxLate = 0, maxEarly = 0;
		for (short val : schedulingDelays) {
			avg += val / (double) (schedulingDelays.size());
			absAvg += Math.abs(val) / (double) (schedulingDelays.size());
			if (val > maxLate)
				maxLate = val;
			if (val < maxEarly)
				maxEarly = val;
		}
		LOG.info("Scheduling delays (0 is optimal): earliest=" + maxEarly
				+ ", latest=" + maxLate + ", average=" + avg
				+ ", average of absolutes=" + absAvg + ", total number="
				+ schedulingDelays.size());
	}

	/**
	 * writes the scheduling delay time series to disk
	 */
	public static void exportMicroStatistics() {
		try (PrintWriter pw = new PrintWriter("microstats.txt");) {
			for (short s : schedulingDelays)
				pw.println(s);
			pw.flush();
			pw.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
