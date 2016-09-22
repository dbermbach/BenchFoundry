/**
 * 
 */
package de.tuberlin.ise.benchfoundry.tracegeneration.consistencybenchmark;

/**
 * this class stores some static text to avoid bloating the actual trace
 * generator file
 * 
 * @author Dave
 *
 */
public class StaticContent {

	/* expert variables */
	/**
	 * the number of business process instances per client that will run
	 * concurrently
	 */
	public static final int concurrentProcessesPerMeasurementClient = 2;

	/**
	 * estimated latency of read operations, determines the runtime of read
	 * processes together with the staleness bounds
	 */
	public static final int estimatedReadLatencyInMs = 1;

	/**
	 * estimated maximum staleness values. determines the runtime of read
	 * processes and the delay between insert and update operation
	 */
	public static final int estimatedStalenessUpperBoundInMs = 10000;

	/**provisioned duration of the warmup trace, default: 5mins*/
	public static final long warmupDurationInMs= 300000;
	
	/**number of threads executing warmup trace*/
	public static final int noOfWarmupThreads = 8;
	
	/**the number of reads in a warmup process*/
	public static final int readsPerWarmupProcess = 100;
	

	public static String filenameSchema = "consbench_schema";
	public static String filenameOpList = "consbench_ops";
	public static String filenameParamList = "consbench_params";
	public static String filenameCustParamList = "consbench_custparams";
	public static String filenamePreloadTrace = "consbench_preload_trace";
	public static String filenameWarmupTrace = "consbench_warmup_trace";
	public static String filenameExperimentTrace = "consbench_exp_trace";
	public static String filenameSuffix = ".txt";

	public static String getFileInfoText() {
		return "\n########################################################################################"
				+ "\n#This BenchFoundry input file is part of set of BenchFoundry input files generated for\n#"
				+ "consistency benchmarking as described in [1-4]. If you use this benchmark\n#"
				+ "for a scientific publication, we would appreciate if you would cite one of\n#"
				+ "those references.\n#\n#"
				+ "[1] D. Bermbach, S. Tai. \"Eventual consistency: How soon is eventual?\n#"
				+ "An evaluation of Amazon S3's consistency behavior\". In: Proceedings of\n#"
				+ "MW4SOC 2011. ACM 2011.\n#"
				+ "[2] D. Bermbach, L. Zhao, S. Sakr. \"Towards Comprehensive Measurement\n#"
				+ "of Consistency Guarantees for Cloud-Hosted Data Storage Services\". In:\n#"
				+ "Proceedings of TPCTC 2013. Springer 2014.\n#"
				+ "[3] D. Bermbach, S. Tai. \"Benchmarking Eventual Consistency: Lessons\n#"
				+ "Learned from Long-Term Experimental Studies\". In: Proceedings of IC2E 2014."
				+ "\n#IEEE 2014.\n#"
				+ "[4] D. Bermbach. \"Benchmarking Eventually Consistent Distributed Storage\n#"
				+ "Systems\". PhD Thesis. Karlsruhe Institute of Technology 2014.\n#\n#"
				+ "When running this benchmark, please, make sure that you have enough measurement\n#"
				+ "machines which should be located as close to the replicas as possible. Less\n#"
				+ "machines or poor machine placement will strongly affect inaccuracies of measurements.\n#"
				+ "Also make sure that you have NTP (or a comparable clock synchronization protocol)"
				+ "\n#running for at least 3 hours before starting the actual benchmark."
				+ "\n########################################################################################";
	}

	public static String getInfoText() {
		return "This trace generator creates traces and other BenchFoundry input files for\n"
				+ "consistency benchmarking as described in [1-4]. If you use this benchmark\n"
				+ "for a scientific publication, we would appreciate if you would cite one of\n"
				+ "those references.\n\n"
				+ "[1] D. Bermbach, S. Tai. \"Eventual consistency: How soon is eventual?\n"
				+ "An evaluation of Amazon S3's consistency behavior\". In: Proceedings of\n"
				+ "MW4SOC 2011. ACM 2011.\n"
				+ "[2] D. Bermbach, L. Zhao, S. Sakr. \"Towards Comprehensive Measurement\n"
				+ "of Consistency Guarantees for Cloud-Hosted Data Storage Services\". In:\n"
				+ "Proceedings of TPCTC 2013. Springer 2014.\n"
				+ "[3] D. Bermbach, S. Tai. \"Benchmarking Eventual Consistency: Lessons\n"
				+ "Learned from Long-Term Experimental Studies\". In: Proceedings of IC2E 2014."
				+ "\nIEEE 2014.\n"
				+ "[4] D. Bermbach. \"Benchmarking Eventually Consistent Distributed Storage\n"
				+ "Systems\". PhD Thesis. Karlsruhe Institute of Technology 2014.\n\n"
				+ "When running this benchmark, please, make sure that you have enough measurement\n"
				+ "machines which should be located as close to the replicas as possible. Less\n"
				+ "machines or poor machine placement will strongly affect inaccuracies of measurements.\n"
				+ "Also make sure that you have NTP (or a comparable clock synchronization protocol)"
				+ "\nrunning for at least 3 hours before starting the actual benchmark.";
	}

	public static String getSeparatorLine() {
		return "---------------------------------------------------------------------------------------------------------";
	}
}
