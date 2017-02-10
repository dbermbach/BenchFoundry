/**
 * 
 */
package de.tuberlin.ise.benchfoundry.util;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.tuberlin.ise.benchfoundry.connectors.IDbConnector;
import de.tuberlin.ise.benchfoundry.connectors.impl.DerbyRelationalConnector;
import de.tuberlin.ise.benchfoundry.physicalschema.factory.AbstractPhysicalSchemaFactory;
import de.tuberlin.ise.benchfoundry.physicalschema.factory.RelationalPhysicalSchemaFactory;
import de.tuberlin.ise.benchfoundry.physicalschema.model.AbstractPhysicalSchema;
import de.tuberlin.ise.benchfoundry.physicalschema.model.AbstractRequest;
import de.tuberlin.ise.benchfoundry.physicalschema.model.columnstore.CSTable;
import de.tuberlin.ise.benchfoundry.results.ResultLogger;
import de.tuberlin.ise.benchfoundry.rpc.RPCHost;
import de.tuberlin.ise.benchfoundry.scheduling.BusinessProcess;
import de.tuberlin.ise.benchfoundry.scheduling.SequentialScheduler;

/**
 * this class holds globally available configuration information
 * 
 * @author Dave
 *
 */
public class BenchFoundryConfigData {

	private static final Logger LOG = LogManager
			.getLogger(BenchFoundryConfigData.class);

	/** file with DDL instructions for the logical schema */
	public static String schemaFilename = "samples/tpc-c_schema";

	/** file containing the list of all SQL queries */
	public static String oplistFilename = "samples/oplist_example.txt";

	/** file containing all parameter lists */
	public static String paramFilename = "samples/paramlist_example.txt";

	/** file containing all custom parameter lists */
	public static String custparamFilename = paramFilename;

	/** file containing the trace for the actual experiment [phase 3] */
	public static String traceFilenameExperiment = "samples/trace_example.txt";

	/**
	 * file containing the separate trace that shall be run prior to the actual
	 * benchmarking trace (should only contain read queries as it will be
	 * terminated at a non-deterministic point in the trace to start the actual
	 * experiment trace) [phase 2]
	 */
	public static String traceFilenameWarmup = "samples/trace_example.txt";

	/**
	 * file containing the trace for preloading the database with some data
	 * [phase 1]
	 */
	public static String traceFilenamePreload = "samples/trace_example.txt";

	/** the dbconnector-specific config file */
	public static String dbConnectorConfigFile = "mariadb.properties";

	/** Path to directory that contains obtained measurements. */
	public static String resultDir = "results";

	/**
	 * factory that creates the physical schema and the corresponding instances
	 * of {@link AbstractRequest}
	 */
	public static Class<? extends AbstractPhysicalSchemaFactory> physicalSchemaFactoryClass = RelationalPhysicalSchemaFactory.class;

	/** physical schema */
	public static AbstractPhysicalSchema physicalSchema;

	/**
	 * class of the connector to the actual data store
	 */
	public static Class<? extends IDbConnector> dbConnectorClass = DerbyRelationalConnector.class;

	/** connector to the actual data store */
	public static IDbConnector dbConnector;

	/**
	 * determines how early transactions are prepared in the {@link DbConnector}
	 * prior to their scheduled execution time
	 */
	public static long transactionPrepareTimeInMs = 1000;

	/**
	 * additional time that a {@link BusinessProcess} is scheduled before
	 * transactions are prepared
	 */
	public static long processScheduleAheadTimeInMs = 500;

	/**
	 * saves the connection information to all BenchFoundry slave instances, is
	 * only set on master
	 */
	public final static Map<String, RPCHost> slaves = new HashMap<String, RPCHost>();

	/** the ID/name of the BenchFoundry instance */
	public static String name = "Master";

	/** describes whether this is the master instance */
	public static boolean masterInstance = true;

	/** if true the master is the only BenchFoundry instance */
	public static boolean singleNode = false;

	/** the port that will be used by BenchFoundry slave instances */
	public static int localPortForSlaves = 9090;

	/** if true {@link SelectiveLogEntry} will be used by {@link BusinessProcess} */
	public static boolean doDetailledLoggingForExceptions = true;

	/**
	 * if true a {@link SequentialScheduler} instance will be used in the
	 * experiment phase
	 */
	public static boolean closedWorkloadSchedulerInExperiment = false;

	/**
	 * if closedWorkloadSchedulerInExperiment==true, this field determines the
	 * size of the thread pool used. Ignored otherwise.
	 */
	public static int closedWorkloadSchedulerThreadpoolSize = 8;

	/* here come all non-serialized, default (expert) parameters */

	/**
	 * after scheduling the last process, the scheduler will wait until this
	 * time has elapsed before killing all processes that are still running.
	 */
	public static final int _maximumProcessDurationInSeconds = 120;

	/**
	 * the scheduler will read entries from the trace into a buffer that are at
	 * least this far in the future (plus schedule ahead and tx prepare
	 * durations)
	 */
	public static final long _schedulerReadBufferInMs = 2000;

	/** the thread pool size in class {@link SequentialScheduler} */
	public static final int _defaultNumberOfThreadsInSequentialScheduler = 8;

	/**
	 * if not all slaves are ready, the master will retry the process after a
	 * time period specified in this variable
	 */
	public static final int _startTimestampAgreementRetryIntervalInSeconds = 30;

	/**
	 * slaves will only agree to an experiment start timestamp that is at least
	 * this far in the future
	 */
	public static final long _minimumExperimentPreparationTimeOnSlavesInMs = 20000;

	/**
	 * the interval in which the {@link ResultLogger} will poll from its input
	 * queue
	 */
	public static final long _resultLoggerPollIntervalInMs = 50;

	/**
	 * during the creation of column store physical schemas, this is the maximum
	 * number of columns by which a {@link CSTable} may grow through a merge
	 */
	public static final int _csPhysicalSchemaGenerationColumnCountMergeThreshold = 10;

	/**
	 * defines the number of secondary index columns supported in physical
	 * schema column store tables
	 */
	public static final int _csPhysicalSchemaMaxNoOfSecIndexColumns = 1;

	/* end of non-serialized expert parameters */

	/**
	 * 
	 * @param name
	 *            the name of the target slave
	 * 
	 * @return a wire representation of this class for sending to slaves
	 */
	public static byte[] serializeConfigData(String name) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();

		try {
			ObjectOutputStream oos = new ObjectOutputStream(baos);
			oos.writeObject(schemaFilename);
			oos.writeObject(oplistFilename);
			oos.writeObject(paramFilename);
			oos.writeObject(custparamFilename);
			oos.writeObject(traceFilenameExperiment);
			oos.writeObject(traceFilenameWarmup);
			oos.writeObject(traceFilenamePreload);
			oos.writeObject(dbConnectorConfigFile);
			oos.writeObject(resultDir);
			oos.writeObject(physicalSchemaFactoryClass);
			oos.writeObject(physicalSchemaFactoryClass.newInstance()
					.serializeSchema(physicalSchema));
			oos.writeObject(dbConnectorClass);
			oos.writeObject(dbConnector.serializeConnectorState());
			oos.writeObject(transactionPrepareTimeInMs);
			oos.writeObject(processScheduleAheadTimeInMs);
			oos.writeObject(name);
			oos.writeObject(new Boolean(doDetailledLoggingForExceptions));
			oos.writeObject(new Boolean(closedWorkloadSchedulerInExperiment));
			oos.writeObject(closedWorkloadSchedulerThreadpoolSize);
			oos.close();
			return baos.toByteArray();

		} catch (Exception e) {
			e.printStackTrace();
		}

		return null;
	}

	/**
	 * deserializes and sets class members of this class according to the output
	 * from serializeConfigData()
	 * 
	 * @param in
	 */
	public static void deserializeAndApplyConfigData(byte[] in) {
		try {
			ObjectInputStream ois = new ObjectInputStream(
					new ByteArrayInputStream(in));
			schemaFilename = (String) ois.readObject();
			oplistFilename = (String) ois.readObject();
			paramFilename = (String) ois.readObject();
			custparamFilename = (String) ois.readObject();
			traceFilenameExperiment = (String) ois.readObject();
			traceFilenameWarmup = (String) ois.readObject();
			traceFilenamePreload = (String) ois.readObject();
			dbConnectorConfigFile = (String) ois.readObject();
			resultDir = (String) ois.readObject();
			physicalSchemaFactoryClass = (Class<? extends AbstractPhysicalSchemaFactory>) ois
					.readObject();
			physicalSchema = physicalSchemaFactoryClass.newInstance()
					.deserializeSchema((byte[]) ois.readObject());
			dbConnectorClass = (Class<? extends IDbConnector>) ois.readObject();
			dbConnector = dbConnectorClass.newInstance();
			dbConnector
					.applySerializedConnectorState((byte[]) ois.readObject());
			transactionPrepareTimeInMs = (long) ois.readObject();
			processScheduleAheadTimeInMs = (long) ois.readObject();
			name = (String) ois.readObject();
			doDetailledLoggingForExceptions = (Boolean) ois.readObject();
			closedWorkloadSchedulerInExperiment = (Boolean) ois.readObject();
			closedWorkloadSchedulerThreadpoolSize = (int) ois.readObject();
			ois.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * reads the specified config file and sets the corresponding fields of this
	 * class
	 * 
	 * @param filename
	 * @throws Exception
	 *             may throw IOExceptions while reading or any number of runtime
	 *             exceptions caused by invalidly formatted config files
	 */
	public static void readFromFile(String filename) throws Exception {
		BufferedReader br = new BufferedReader(new FileReader(filename));
		String line;
		String[] splits;
		while ((line = br.readLine()) != null) {
			if (line.trim().length() == 0 || line.trim().startsWith("#"))
				continue;
			splits = line.split(":");
			if (splits.length != 2)
				continue;
			switch (splits[0].trim()) {
			case "schemaFilename":
				schemaFilename = splits[1].trim();
				LOG.info("Using schema from file " + schemaFilename);
				break;
			case "oplistFilename":
				oplistFilename = splits[1].trim();
				LOG.info("Using queries from oplist file " + oplistFilename);
				break;
			case "paramFilename":
				paramFilename = splits[1].trim();
				LOG.info("Using parameters from file " + paramFilename);
				break;
			case "custparamFilename":
				custparamFilename = splits[1].trim();
				if (custparamFilename.equalsIgnoreCase("none")) {
					custparamFilename = "custparam.tmp";
					IOUtils.writeFile("custparam.tmp",
							"#dummy content generated by BenchFoundry\n-1:-1"
									.getBytes());
					LOG.info("Using no custom parameters.");
				} else
					LOG.info("Using custom parameters from file "
							+ custparamFilename);
				break;
			case "preloadTraceFilename":
				traceFilenamePreload = splits[1].trim();
				if (traceFilenamePreload.equalsIgnoreCase("none")) {
					traceFilenamePreload = "preload.tmp";
					IOUtils.writeFile("preload.tmp",
							"#dummy content generated by BenchFoundry"
									.getBytes());
					LOG.info("Using no preload trace.");
				} else
					LOG.info("Using preload trace from file "
							+ traceFilenamePreload);
				break;
			case "warmupTraceFilename":
				traceFilenameWarmup = splits[1].trim();
				if (traceFilenameWarmup.equalsIgnoreCase("none")) {
					traceFilenameWarmup = "warmup.tmp";
					IOUtils.writeFile("warmup.tmp",
							"#dummy content generated by BenchFoundry"
									.getBytes());
					LOG.info("Using no warmup trace.");
				} else
					LOG.info("Using warm up trace from file "
							+ traceFilenameWarmup);
				break;
			case "experimentTraceFilename":
				traceFilenameExperiment = splits[1].trim();
				LOG.info("Using experiment trace from file "
						+ traceFilenameExperiment);
				break;
			case "dbConnectorConfigFile":
				dbConnectorConfigFile = splits[1].trim();
				LOG.info("Using dbconnector config data from file "
						+ dbConnectorConfigFile);
				break;
			case "resultDir":
				resultDir = splits[1].trim();
				LOG.info("Setting result output directory to " + resultDir);
				break;
			case "physicalSchemFactoryClass":
				physicalSchemaFactoryClass = (Class<? extends AbstractPhysicalSchemaFactory>) Class
						.forName(splits[1].trim());
				LOG.info("Using schema factory class "
						+ physicalSchemaFactoryClass.getSimpleName());
				break;
			case "dbConnectorClass":
				dbConnectorClass = (Class<? extends IDbConnector>) Class
						.forName(splits[1].trim());
				LOG.info("Using database connector "
						+ dbConnectorClass.getSimpleName());
				break;
			case "transactionPrepareTimeInMs":
				transactionPrepareTimeInMs = Long.parseLong(splits[1].trim());
				LOG.info("Setting transaction prepare time to "
						+ transactionPrepareTimeInMs + "ms");
				break;
			case "processScheduleAheadTimeInMs":
				processScheduleAheadTimeInMs = Long.parseLong(splits[1].trim());
				LOG.info("Setting process schedule ahead time to "
						+ processScheduleAheadTimeInMs + "ms");
				break;
			case "slaveFile":
				if (splits[1].equals("none")) {
					singleNode = true;
				} else
					slaves.putAll(readSlaveFile(splits[1].trim()));
				break;
			case "doDetailledLoggingForExceptions":
				doDetailledLoggingForExceptions = Boolean
						.parseBoolean(splits[1].trim());
				LOG.info("Detailled logging for business processes with exceptions has been "
						+ (doDetailledLoggingForExceptions ? "enabled."
								: "disabled."));
				break;
			case "closedWorkloadSchedulerInExperimentPhase":
				closedWorkloadSchedulerInExperiment = Boolean
						.parseBoolean(splits[1].trim());
				LOG.info("Using a closed workload scheduler for the experiment phase has been "
						+ (doDetailledLoggingForExceptions ? "enabled."
								: "disabled."));
				break;
			case "closedWorkloadSchedulerThreadpoolSize":
				closedWorkloadSchedulerThreadpoolSize = Integer
						.parseInt(splits[1].trim());
				if (closedWorkloadSchedulerThreadpoolSize < 1) {
					LOG.info("A threadpool size of "
							+ closedWorkloadSchedulerThreadpoolSize
							+ " is not supported, defaulting to 8.");
					closedWorkloadSchedulerThreadpoolSize = 8;
				}
				LOG.info("If a closed workload scheduler is used in the experiment phase, it will run "
						+ closedWorkloadSchedulerThreadpoolSize
						+ " business processes in parallel.");
				break;
			}
		}

	}

	/**
	 * @param filename
	 * @return
	 */
	private static Map<String, RPCHost> readSlaveFile(String filename)
			throws Exception {
		Map<String, RPCHost> result = new HashMap<>();
		BufferedReader br = new BufferedReader(new FileReader(filename));
		String line;
		String[] splits;
		while ((line = br.readLine()) != null) {
			if (line.trim().length() == 0 || line.trim().startsWith("#"))
				continue;
			splits = line.split(";");
			if (splits.length != 3)
				continue;
			RPCHost rpc = new RPCHost(splits[1].trim(),
					Integer.parseInt(splits[2].trim()), splits[0].trim());
			result.put(rpc.name, rpc);
			LOG.info("Registered slave: " + rpc);
		}
		if (result.size() > 0)
			LOG.info("Running BenchFoundry with one master and "
					+ result.size() + " slave instance(s).");
		else {
			singleNode = true;
			LOG.info("Running BenchFoundry as single node instance.");
		}
		br.close();
		return result;
	}

}
