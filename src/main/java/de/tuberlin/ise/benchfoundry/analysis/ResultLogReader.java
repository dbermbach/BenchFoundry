/**
 */
package de.tuberlin.ise.benchfoundry.analysis;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.tuberlin.ise.benchfoundry.results.BusinessOperationResult;
import de.tuberlin.ise.benchfoundry.util.BenchFoundryConfigData;

/**
 * @author Akon Dey (akon.dey@gmail.com)
 *
 */
public class ResultLogReader {
	private static final Logger LOG = LogManager.getLogger(ResultLogReader.class);
	private static final String RESULT_LOG = "result.log";

	private static Charset charset = Charset.forName("UTF-8");

	/** singleton object */
	private static ResultLogReader instance;

	public class Metric {
		private int qid;
		private String name;
		private long count = 0;
		private long max = Long.MIN_VALUE;
		private long min = Long.MAX_VALUE;

		public static final String EMPTY_METRIC_CSV = ",,";

		public Metric(int qid, String name) {
			this.qid = qid;
			this.name = name;
		}

		public String toCSV() {
			return this.max + "," + this.min + "," + this.count;
		}

		public String toCSVHeader() {
			return this.qid + ":max_time," + this.qid + ":min_time," + this.qid + ":num_ops";
		}

		public String getName() {
			return this.name;
		}

		/**
		 * @param result
		 */
		public void count(BusinessOperationResult result) {
			this.count++;
			long diff = result.getOperationEnd() - result.getOperationStart();
			if (this.max < diff)
				this.max = diff;
			if (this.min > diff)
				this.min = diff;
		}
	}

	public static long MILLISECONDS_IN_MINUTE = 60;

	private Map<Long, Map<Integer, Metric>> allMetrics = new TreeMap<Long, Map<Integer, Metric>>();

	public void update(BusinessOperationResult result) {
		long minute = (result.getOperationStart() / MILLISECONDS_IN_MINUTE) * MILLISECONDS_IN_MINUTE;
		int queryId = result.getLogicalQueryId();
		Map<Integer, Metric> metrics = this.allMetrics.get(minute);
		if (metrics == null) {
			this.allMetrics.put(minute, metrics = new TreeMap<Integer, Metric>());
		}
		Metric metric = metrics.get(queryId);
		if (metric == null) {
			metrics.put(queryId, metric = new Metric(result.getLogicalQueryId(), "num ops"));
		}
		metric.count(result);
	}

	public void load(String fileName) {
		Path out = Paths.get(BenchFoundryConfigData.resultDir).resolve(fileName);
		String line;
		try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
			while ((line = reader.readLine()) != null) {
				BusinessOperationResult result = BusinessOperationResult.fromCsv(line);
				this.update(result);
			}
			reader.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void dump(String dumpFile) {
		long totalTime = 0;
		try (PrintWriter writer = new PrintWriter(new FileWriter(dumpFile))) {
			Set<Integer> queryIds = new TreeSet<Integer>();
			Set<Long> minutes = new TreeSet<Long>();

			for (long minute : this.allMetrics.keySet()) {
				minutes.add(minute);
				Map<Integer, Metric> metrics = this.allMetrics.get(minute);
				for (int queryId : metrics.keySet()) {
					queryIds.add(queryId);
				}
			}

			String headerOutput = "";
			for (int queryId : queryIds) {
				for (long minute : minutes) {
					Metric metric = this.allMetrics.get(minute).get(queryId);
					if (metric != null) {
						headerOutput += metric.toCSVHeader() + ",";
						break;
					}
				}
			}
			headerOutput = headerOutput.substring(0, headerOutput.length() - 1);
			writer.println(headerOutput);

			String output = "";
			for (long minute : minutes) {
				output = "";
				for (int queryId : queryIds) {
					Metric metric = this.allMetrics.get(minute).get(queryId);
					if (metric != null) {
						output += metric.toCSV() + ",";
					} else {
						output += Metric.EMPTY_METRIC_CSV + ",";
					}
				}
				output = output.substring(0, output.length() - 1);
				writer.println(output);
			}
			writer.flush();
			writer.close();
			System.out.println("Total runtime for all operations: " + totalTime);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}