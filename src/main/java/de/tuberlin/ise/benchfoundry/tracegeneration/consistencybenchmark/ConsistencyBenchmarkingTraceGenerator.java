/**
 * 
 */
package de.tuberlin.ise.benchfoundry.tracegeneration.consistencybenchmark;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import de.tuberlin.ise.benchfoundry.util.Time;

/**
 * @author Dave
 *
 */
public class ConsistencyBenchmarkingTraceGenerator {

	/* variables based on input */

	/** approximate number of replicas in the storage system */
	private static int noOfReplicas;

	/**
	 * number of measurement clients needed to get sufficiently significant
	 * results
	 */
	private static int noOfMeasurementClients;

	/** total runtime as specified by the resulting trace */
	private static long benchmarkDurationInMs = 0;

	/** total number of update sequences on a separate key each */
	private static long noOfStalenessTests = 10000;

	/**
	 * caches all experiment trace entries before writing down to allow out of
	 * order creation
	 */
	private static SortedSet<String> orderedProcesses = new TreeSet<>();

	/**
	 * caches all preload trace entries before writing down to allow out of
	 * order creation
	 */
	private static SortedSet<String> orderedPreloadProcesses = new TreeSet<>();

	/**
	 * caches all preload trace entries before writing down to allow out of
	 * order creation
	 */
	private static SortedSet<String> orderedWarmupProcesses = new TreeSet<>();

	/** caches all key to update timestamps mapping of the main test */
	private static Map<Long, Long> updateTimestamps = new HashMap<>();

	/** caches all entries for the param list */
	private static Map<Long, String> params = new HashMap<>();

	/** assert unique process ids */
	private static long nextProcessId = 1;

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		System.out.println(StaticContent.getInfoText());
		System.out.println(StaticContent.getSeparatorLine());
		System.out
				.println("Please, enter the total duration of the test:\n"
						+ "(format: 1w 2d 3h 4m for a test run that approximately runs one week, 2 days, 3 hours, and 4 minutes)");
		Scanner in = new Scanner(System.in);
		String inputRead;
		do {
			inputRead = in.nextLine();
		} while ((benchmarkDurationInMs = Utils.parseDuration(inputRead)) == -1);
		System.out.println("Benchmark duration set to " + benchmarkDurationInMs
				+ "ms.");
		System.out.println("Please, enter the approximate number of replicas");
		noOfReplicas = (int) Utils.readLong(in);
		System.out
				.println("Please, enter the total number of staleness tests (update-[read]+)");
		noOfStalenessTests = Utils.readLong(in);
		System.out
				.println("Please, enter the desired minimum probability of reaching all"
						+ " replicas when all readers read a random\nreplica concurrently");
		double minProb = Utils.readProbability(in), prob = 0;
		noOfMeasurementClients = noOfReplicas - 1;
		while (prob < minProb) {
			prob = Utils.calculateProbabilityOfReachingAllReplicas(
					noOfReplicas, ++noOfMeasurementClients);
		}
		System.out.println("Using " + noOfMeasurementClients++
				+ " readers will result in a probability of " + prob);
		// ++ is necessary since we need an additional writer

		prepareTraces();
		writeSchema();
		writeOpsList();
		writeCustParamList();
		writeParamList();
		writePreloadTrace();
		writeWarmupTrace();
		writeExperimentTrace();

	}

	private static void prepareTraces() {
		// prepare writes for experiment and preload trace
		for (long i = 0; i < noOfStalenessTests; i++) {
			long pid = nextProcessId++;
			long startOffset = i
					* Math.round((double) benchmarkDurationInMs
							/ noOfStalenessTests);
			updateTimestamps.put(pid, startOffset);
			orderedProcesses.add(buildUpdateProcess(startOffset, pid));
			params.put(pid, startOffset + ";" + pid); // for update
			params.put(-1 * pid, pid + ";-1"); // init value for insert
			orderedPreloadProcesses.add(buildInsertProcess(0, -1 * pid));
		}
		// prepare reads for experiment trace
		Set<Long> paramKeys = new HashSet<>();
		for (long key : updateTimestamps.keySet()) {
			long updTime = updateTimestamps.get(key);
			long paramKey = nextProcessId;
			params.put(nextProcessId, "" + key); // for reads
			paramKeys.add(nextProcessId);
			for (int i = 0; i < StaticContent.concurrentProcessesPerMeasurementClient
					* noOfMeasurementClients; i++)
				orderedProcesses.add(buildReadProcess(updTime, nextProcessId++,
						paramKey));
		}
		// prepare reads for warmup trace
		Long[] pKeys = paramKeys.toArray(new Long[paramKeys.size()]);
		long counter = 0;
		while (counter < StaticContent.warmupDurationInMs) {
			for (int i = 0; i < StaticContent.noOfWarmupThreads
					* noOfMeasurementClients; i++) {
				orderedWarmupProcesses.add(buildRandomReadsProcess(counter,
						nextProcessId++, StaticContent.readsPerWarmupProcess,
						pKeys));
			}
			counter += StaticContent.readsPerWarmupProcess
					* StaticContent.estimatedReadLatencyInMs;
		}
		System.out.println("Prepared a total of " + orderedProcesses.size()
				+ " experiment trace entries,\n"
				+ orderedPreloadProcesses.size() + " preload trace entries, "
				+ orderedWarmupProcesses.size() + " warmup trace entries, and "
				+ params.size() + " parameters.");
	}

	private static String buildInsertProcess(long startOffset, long processId) {
		StringBuilder sb = new StringBuilder("BOP;" + startOffset + ";"
				+ processId);
		sb.append("\nBOT;0");
		sb.append("\n1;" + processId); // insert
		sb.append("\nEOT");
		sb.append("\nEOP");

		return sb.toString();
	}

	private static String buildUpdateProcess(long startOffset, long processId) {
		StringBuilder sb = new StringBuilder("BOP;" + startOffset + ";"
				+ processId);
		sb.append("\nBOT;0");
		sb.append("\n2;" + processId); // update
		sb.append("\nEOT");
		sb.append("\nEOP");

		return sb.toString();
	}

	private static String buildReadProcess(long writeStartOffset,
			long processId, long targetKey) {
		StringBuilder sb = new StringBuilder("BOP;" + (writeStartOffset) + ";"
				+ processId);
		long counter = -50;
		while (counter < 50 + StaticContent.estimatedStalenessUpperBoundInMs) {
			sb.append("\nBOT;0");
			sb.append("\n3;" + targetKey); // read
			sb.append("\nEOT");
			counter += StaticContent.estimatedReadLatencyInMs;
		}
		sb.append("\nEOP");
		return sb.toString();
	}

	private static String buildRandomReadsProcess(long startOffset,
			long processId, int noOfReads, Long[] pKeys) {
		StringBuilder sb = new StringBuilder("BOP;" + startOffset + ";"
				+ processId);
		sb.append("\nBOT;0");
		for (int i = 0; i < noOfReads; i++) {
			sb.append("\n3;" + pKeys[(int) (Math.random() * pKeys.length)]); // read
		}
		sb.append("\nEOT");
		sb.append("\nEOP");
		return sb.toString();
	}

	/**
	 * writes the preload trace
	 */
	private static void writePreloadTrace() throws IOException {
		PrintWriter pw = new PrintWriter(StaticContent.filenamePreloadTrace
				+ StaticContent.filenameSuffix);
		pw.println("#preload trace file");
		writeHeader(pw);
		for (String proc : orderedPreloadProcesses)
			pw.println(proc);
		pw.close();
		System.out.println("Wrote preload trace.");
	}

	/**
	 * writes the warm up trace
	 */
	private static void writeWarmupTrace() throws IOException {
		PrintWriter pw = new PrintWriter(StaticContent.filenameWarmupTrace
				+ StaticContent.filenameSuffix);
		pw.println("#warmup trace file");
		writeHeader(pw);

		for (String proc : orderedWarmupProcesses)
			pw.println(proc);

		pw.close();
		System.out.println("Wrote warmup trace.");
	}

	/**
	 * writes the experiment trace
	 */
	private static void writeExperimentTrace() throws IOException {
		PrintWriter pw = new PrintWriter(StaticContent.filenameExperimentTrace
				+ StaticContent.filenameSuffix);
		pw.println("#experiment trace file");
		writeHeader(pw);

		for (String proc : orderedProcesses)
			pw.println(proc);

		pw.close();
		System.out.println("Wrote experiment trace.");
	}

	/**
	 * writes the op list
	 */
	private static void writeOpsList() throws IOException {
		PrintWriter pw = new PrintWriter(StaticContent.filenameOpList
				+ StaticContent.filenameSuffix);
		pw.println("#op list file");
		writeHeader(pw);
		pw.println("1:INSERT INTO consbench VALUES (?,?);");
		pw.println("2:UPDATE consbench SET myfield=? WHERE mykey=?;");
		pw.println("3:SELECT myfield FROM consbench WHERE mykey=?;");
		pw.close();
	}

	/**
	 * writes an (empty) cust param file
	 * 
	 */
	private static void writeCustParamList() throws IOException {
		PrintWriter pw = new PrintWriter(StaticContent.filenameCustParamList
				+ StaticContent.filenameSuffix);
		pw.println("#cust param list file, unused file with dummy content");
		writeHeader(pw);
		pw.println("-1 : -1");
		pw.close();
	}

	/**
	 * writes the param list input file
	 * 
	 * @throws IOException
	 * 
	 * 
	 */
	private static void writeParamList() throws IOException {
		PrintWriter pw = new PrintWriter(StaticContent.filenameParamList
				+ StaticContent.filenameSuffix);
		pw.println("#param list file");
		writeHeader(pw);
		for (Entry<Long, String> entry : params.entrySet()) {
			pw.println(entry.getKey() + ": " + entry.getValue());
		}

		pw.close();
		System.out.println("Wrote param list file.");
	}

	/**
	 * writes the schema input file
	 * 
	 * @throws IOException
	 * 
	 */
	private static void writeSchema() throws IOException {
		PrintWriter pw = new PrintWriter(StaticContent.filenameSchema
				+ StaticContent.filenameSuffix);
		pw.println("#schema file");
		writeHeader(pw);
		pw.println("1:CREATE TABLE consbench (mykey int, myfield varchar(64));");
		pw.close();
		System.out.println("Wrote schema file.");
	}

	/**
	 * writes some comments into the file header
	 * 
	 * @param pw
	 * @throws IOException
	 */
	private static void writeHeader(PrintWriter pw) throws IOException {
		pw.println(StaticContent.getFileInfoText());
		pw.println("#\n#generated on: " + new Date());
		pw.println("#approximate number of database replicas: " + noOfReplicas);
		pw.println("#number of required measurement machines: "
				+ noOfMeasurementClients);
		pw.println("#number of read threads per machine per test: "
				+ StaticContent.concurrentProcessesPerMeasurementClient);
		pw.println("#number of staleness tests: " + noOfStalenessTests);
		pw.println("#delay between tests: "
				+ Time.formatDuration(Math.round((double) benchmarkDurationInMs
						/ noOfStalenessTests)));
		pw.println("#approximate benchmark duration: "
				+ Time.formatDuration(benchmarkDurationInMs));
		pw.println("#estimated staleness upper bound: "
				+ Time.formatDuration(StaticContent.estimatedStalenessUpperBoundInMs));
		pw.println("#estimated read latency in ms: "
				+ StaticContent.estimatedReadLatencyInMs);
	}

}
