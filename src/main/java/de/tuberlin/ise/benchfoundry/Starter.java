/**
 * 
 */
package de.tuberlin.ise.benchfoundry;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.parser.ParseException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;

import de.tuberlin.ise.benchfoundry.logicalschema.model.DataSchema;
import de.tuberlin.ise.benchfoundry.logicalschema.queries.ParameterRegistry;
import de.tuberlin.ise.benchfoundry.rpc.BenchmarkSlaveServer;
import de.tuberlin.ise.benchfoundry.rpc.MasterToSlaveCommunicator;
import de.tuberlin.ise.benchfoundry.rpc.SlaveNotificationInbox;
import de.tuberlin.ise.benchfoundry.scheduling.Scheduler;
import de.tuberlin.ise.benchfoundry.scheduling.SequentialScheduler;
import de.tuberlin.ise.benchfoundry.util.BenchFoundryConfigData;
import de.tuberlin.ise.benchfoundry.util.IOUtils;
import de.tuberlin.ise.benchfoundry.util.MicroStatisticsCollector;
import de.tuberlin.ise.benchfoundry.util.Phase;
import de.tuberlin.ise.benchfoundry.util.PhaseManager;
import de.tuberlin.ise.benchfoundry.util.Time;
import de.tuberlin.ise.benchfoundry.util.TraceFileSplitter;

/**
 * @author Dave
 *
 */
public class Starter {

	private static final Logger LOG = LogManager.getLogger(Starter.class);

	/**
	 * @param args
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws InterruptedException {
		configureLog4j2();
		if (args.length == 0 || !args[0].matches("[\\d]+")) {
			LOG.info("Starting BenchFoundry master.");
			BenchFoundryConfigData.masterInstance = true;
			runMaster(args);
		} else {
			LOG.info("Starting BenchFoundry slave.");
			BenchFoundryConfigData.masterInstance = false;
			runSlave(args);
		}

	}

	/**
	 * @throws InterruptedException
	 * 
	 */
	private static void runSlave(String[] args) throws InterruptedException {
		int port = 9090;
		if (args.length > 0) {
			try {
				port = Integer.parseInt(args[0]);
			} catch (NumberFormatException e) {
				LOG.error("Could not parse first parameter 'port' (was:"
						+ args[0] + "), defaulting to 9090: " + e.getMessage(),
						e);
			}
		} else
			LOG.error("First parameter 'port' was missing"
					+ ", defaulting to 9090.");
		openCommunicationChannelsAtSlave(port);
		LOG.info("Waiting for configuration details and input files now (if not already received).");
		waitForConfigFilesAtSlave();

		// wait for master to signal phase change to PRELOAD phase
		SlaveNotificationInbox.waitForStartPreload();
		runPreloadPhase();
		PhaseManager.getInstance().changePhase(); // change to warmup
		Thread warmupThread = startWarmupPhaseLocally();
		SlaveNotificationInbox.waitForExperimentStarttimeSet();
		// wait until benchmark start, then terminate warm-up phase and start
		// experiment
		Time.waitUntilRelativeTime(-(BenchFoundryConfigData.processScheduleAheadTimeInMs
				+ BenchFoundryConfigData.transactionPrepareTimeInMs + 5));
		Thread experimentThread = startExperiment();
		Time.waitUntilRelativeTime(-5);
		warmupThread.interrupt();
		PhaseManager.getInstance().changePhase(); // change to experiment

		// wait for experiment to complete
		experimentThread.join();

		PhaseManager.getInstance().changePhase(); // change to cleanup
		finalizeMeasurementResults();
		// close connection to SUT
		LOG.info("Closing connection to SUT");
		PhaseManager.getInstance().changePhase();
	}

	/**
	 * 
	 */
	private static void waitForConfigFilesAtSlave() {
		SlaveNotificationInbox.waitForConfigurationDetails();
		SlaveNotificationInbox.waitForCustParamListReceived();
		SlaveNotificationInbox.waitForParamListReceived();
		SlaveNotificationInbox.waitForOplistReceived();
		SlaveNotificationInbox.waitForPreloadTraceReceived();
		SlaveNotificationInbox.waitForSchemaReceived();
		SlaveNotificationInbox.waitForWarmupTraceReceived();
		SlaveNotificationInbox.waitForExperimentTraceReceived();
		SlaveNotificationInbox.waitForDbConnectorConfigReceived();
		LOG.info("All config files received.");
		ParameterRegistry.getInstance().addParamFile(
				BenchFoundryConfigData.paramFilename);
		ParameterRegistry.getInstance().addCustomParamFile(
				BenchFoundryConfigData.custparamFilename);

		// prepare schema
		try {
			DataSchema.getInstance().addSchemaInputFile(
					BenchFoundryConfigData.schemaFilename);
		} catch (IOException | ParseException e1) {
			LOG.error("Could not load logical schema. Reason: "
					+ e1.getMessage());
			e1.printStackTrace();
			System.exit(-1);
		}
		// connect to SUT and set configuration details
		LOG.info("Connecting to SUT");
		BenchFoundryConfigData.dbConnector.init();
	}

	/**
	 * opens thrift communication channels
	 */
	private static BenchmarkSlaveServer openCommunicationChannelsAtSlave(
			int port) {
		LOG.info("Starting benchmark slave server as daemon on port " + port
				+ ".");
		return BenchmarkSlaveServer.startNewSlaveServer(port);
	}

	/**
	 * starts a BenchFoundry master process
	 * 
	 * @throws InterruptedException
	 */
	private static void runMaster(String[] args) throws InterruptedException {
		configureLocalAndSUT(args.length > 0 ? args[0] : null);
		splitTraces();
		forwardConfigToSlaves();

		PhaseManager.getInstance().changePhase(); // change to preload

		if (!BenchFoundryConfigData.singleNode)
			triggerPreloadPhaseOnSlaves();
		runPreloadPhase();

		PhaseManager.getInstance().changePhase(); // change to warmup
		Thread warmupThread = startWarmupPhaseLocally();
		determineExperimentStartTimestamp();
		LOG.debug("(1) Current relative time is " + Time.now());
		// wait until benchmark start, then terminate warm-up phase and start
		// experiment
		Time.waitUntilRelativeTime(-(BenchFoundryConfigData.processScheduleAheadTimeInMs
				+ BenchFoundryConfigData.transactionPrepareTimeInMs + 5));
		LOG.debug("(2) Current relative time is " + Time.now());
		Thread experimentThread = startExperiment();
		LOG.debug("(3) Current relative time is " + Time.now());
		Time.waitUntilRelativeTime(-5);
		LOG.debug("(4) Current relative time is " + Time.now());
		warmupThread.interrupt();
		LOG.debug("(5) Current relative time is " + Time.now());
		PhaseManager.getInstance().changePhase(); // change to experiment
		LOG.debug("(6) Current relative time is " + Time.now());
		// wait for experiment to complete
		experimentThread.join();
		LOG.debug("(7) Current relative time is " + Time.now());

		PhaseManager.getInstance().changePhase(); // change to cleanup
		finalizeMeasurementResults();
		cleanUpSUT();

		if (!BenchFoundryConfigData.singleNode)
			MasterToSlaveCommunicator.getInstance().closeConnections();
		// close connection to SUT
		LOG.info("Closing connection to SUT");
		PhaseManager.getInstance().changePhase(); // change to terminated

	}

	/**
	 * only run on master <br>
	 * 1) reads all config files, creates schemas, queries etc. 2) configures
	 * the SUT
	 * 
	 */
	private static void configureLocalAndSUT(String configFile) {
		// read all config files
		LOG.info("Reading BenchFoundry config files.");
		try {
			if (configFile == null) {
				BenchFoundryConfigData.readFromFile("benchfoundry.properties");
				LOG.info("No config file specified as first parameter, defaulting to benchfoundry.properties");
			} else
				BenchFoundryConfigData.readFromFile(configFile);
		} catch (Exception e2) {
			LOG.error("Could not read config file, terminating: "
					+ e2.getMessage());
			e2.printStackTrace();
			System.exit(-1);
		}

		ParameterRegistry.getInstance().addParamFile(
				BenchFoundryConfigData.paramFilename);
		ParameterRegistry.getInstance().addCustomParamFile(
				BenchFoundryConfigData.custparamFilename);
		ParameterRegistry.getInstance().assertNullParamAvailability();

		// prepare schema
		try {
			DataSchema.getInstance().addSchemaInputFile(
					BenchFoundryConfigData.schemaFilename);
			DataSchema.getInstance().setReadyForUse();
		} catch (IOException | ParseException e1) {
			LOG.error("Could not load logical schema. Reason: "
					+ e1.getMessage());
			e1.printStackTrace();
			System.exit(-1);
		}

		try {
			BenchFoundryConfigData.physicalSchema = BenchFoundryConfigData.physicalSchemaFactoryClass
					.newInstance().createPhysicalSchema();
		} catch (InstantiationException | IllegalAccessException e) {
			LOG.error("Could not instantiate factory class.");
			e.printStackTrace();
			System.exit(-1);
		}

		try {
			BenchFoundryConfigData.dbConnector = BenchFoundryConfigData.dbConnectorClass
					.newInstance();
		} catch (InstantiationException | IllegalAccessException e) {
			LOG.error("Could not instantiate DB Connector.");
			e.printStackTrace();
			System.exit(-1);
		}
		LOG.info("Configuring DB Connector.");
		BenchFoundryConfigData.dbConnector
				.readConfigData(BenchFoundryConfigData.dbConnectorConfigFile);

		LOG.info("Configuring SUT.");
		// create schema on SUT
		BenchFoundryConfigData.dbConnector
				.setupPhysicalSchema(BenchFoundryConfigData.physicalSchema);

		// connect to SUT and set configuration details
		LOG.info("Connecting to SUT");
		BenchFoundryConfigData.dbConnector.init();
	}

	/**
	 * only run on master <br>
	 * 
	 * splits the preload and experiment trace files
	 */
	private static void splitTraces() {
		// preload trace
		if (BenchFoundryConfigData.singleNode) {
			IOUtils.copyFile(BenchFoundryConfigData.traceFilenamePreload,
					BenchFoundryConfigData.traceFilenamePreload
							+ buildTempFileSuffix("master"));
			IOUtils.copyFile(BenchFoundryConfigData.traceFilenameExperiment,
					BenchFoundryConfigData.traceFilenameExperiment
							+ buildTempFileSuffix("master"));
		} else {
			LOG.info("Now splitting preload and experiment traces. This may take a while.");
			List<String> outputFiles = new ArrayList<>();
			for (String name : BenchFoundryConfigData.slaves.keySet())
				outputFiles.add(BenchFoundryConfigData.traceFilenamePreload
						+ buildTempFileSuffix(name));
			outputFiles.add(BenchFoundryConfigData.traceFilenamePreload
					+ buildTempFileSuffix("master"));
			try {
				TraceFileSplitter.splitTrace(
						BenchFoundryConfigData.traceFilenamePreload,
						outputFiles);
			} catch (IOException e) {
				LOG.error(
						"Error while splitting preload trace: "
								+ e.getMessage(), e);
				e.printStackTrace();
				System.exit(-1);
			}
			LOG.info("Finished splitting preload trace into "
					+ outputFiles.size() + " parts.");
			// experiment trace
			outputFiles.clear();
			for (String name : BenchFoundryConfigData.slaves.keySet())
				outputFiles.add(BenchFoundryConfigData.traceFilenameExperiment
						+ buildTempFileSuffix(name));
			outputFiles.add(BenchFoundryConfigData.traceFilenameExperiment
					+ buildTempFileSuffix("master"));
			try {
				TraceFileSplitter.splitTrace(
						BenchFoundryConfigData.traceFilenameExperiment,
						outputFiles);
			} catch (IOException e) {
				LOG.error(
						"Error while splitting experiment trace: "
								+ e.getMessage(), e);
				e.printStackTrace();
				System.exit(-1);
			}
			LOG.info("Finished splitting experiment trace into "
					+ outputFiles.size() + " parts.");
		}
	}

	/**
	 * only run on master <br>
	 * 
	 * sends all traces or trace subsets as well as all configuration data to
	 * the slaves (incl. schemas)
	 */
	private static void forwardConfigToSlaves() {
		// connect to other BenchFoundry instances and forward configuration
		// details
		// incl. schemas etc.
		MasterToSlaveCommunicator.getInstance().openConnections();
		boolean success = true;
		for (String slave : BenchFoundryConfigData.slaves.keySet()) {
			success &= MasterToSlaveCommunicator.getInstance()
					.sendConfigDetails(slave,
							BenchFoundryConfigData.serializeConfigData(slave));
		}
		if (success)
			LOG.info("Configuration details have been forwarded to slaves.");
		else {
			LOG.error("Forwarding configuration details to one or more slaves failed.");
			System.exit(-1);
		}
		success &= IOUtils
				.assertFileExists(BenchFoundryConfigData.custparamFilename);
		success &= IOUtils
				.assertFileExists(BenchFoundryConfigData.paramFilename);
		success &= IOUtils
				.assertFileExists(BenchFoundryConfigData.oplistFilename);
		success &= IOUtils
				.assertFileExists(BenchFoundryConfigData.traceFilenamePreload);
		success &= IOUtils
				.assertFileExists(BenchFoundryConfigData.traceFilenameWarmup);
		success &= IOUtils
				.assertFileExists(BenchFoundryConfigData.traceFilenameExperiment);
		success &= IOUtils
				.assertFileExists(BenchFoundryConfigData.schemaFilename);
		success &= IOUtils
				.assertFileExists(BenchFoundryConfigData.dbConnectorConfigFile);
		if (!success) {
			LOG.error("Not all input files exist.");
			System.exit(-1);
		}

		success = MasterToSlaveCommunicator.getInstance().broadcastSchema(
				IOUtils.readFile(BenchFoundryConfigData.schemaFilename));
		success &= MasterToSlaveCommunicator.getInstance().broadcastOpList(
				IOUtils.readFile(BenchFoundryConfigData.oplistFilename));
		success &= MasterToSlaveCommunicator.getInstance().broadcastParamList(
				IOUtils.readFile(BenchFoundryConfigData.paramFilename));
		success &= MasterToSlaveCommunicator
				.getInstance()
				.broadcastCustParamList(
						IOUtils.readFile(BenchFoundryConfigData.custparamFilename));
		success &= MasterToSlaveCommunicator
				.getInstance()
				.broadcastDbConnectorConfigFile(
						IOUtils.readFile(BenchFoundryConfigData.dbConnectorConfigFile));
		for (String slave : BenchFoundryConfigData.slaves.keySet()) {
			success &= MasterToSlaveCommunicator
					.getInstance()
					.sendPreloadTrace(
							slave,
							IOUtils.readFile(BenchFoundryConfigData.traceFilenamePreload
									+ buildTempFileSuffix(slave)));
			success &= MasterToSlaveCommunicator
					.getInstance()
					.sendWarmupTrace(
							slave,
							IOUtils.readFile(BenchFoundryConfigData.traceFilenameWarmup));
			success &= MasterToSlaveCommunicator
					.getInstance()
					.sendExperimentTrace(
							slave,
							IOUtils.readFile(BenchFoundryConfigData.traceFilenameExperiment
									+ buildTempFileSuffix(slave)));
		}

		if (!success) {
			LOG.error("Failed while forwarding input and trace files to slaves. Terminating.");
			System.exit(-1);
		} else
			LOG.info("All input and trace files have been forwarded to slaves.");

	}

	/**
	 * only run on master<br>
	 * 
	 * tells slave instances to start executing the preload phase and to proceed
	 * to the warmup phase afterwards.
	 */
	private static void triggerPreloadPhaseOnSlaves() {
		if (MasterToSlaveCommunicator.getInstance().commitStartTimestamp(
				Phase.PRELOAD, System.currentTimeMillis())) {
			LOG.info("Signaled slaves to start preload and warmup phase.");
		} else {
			LOG.error("Could not reach slaves for starting preload and warmup phase.");
			System.exit(-1);
		}
	}

	/**
	 * run on both master and slaves <br>
	 * 
	 * fully executes the preload phase before terminating.
	 * 
	 * @throws InterruptedException
	 */
	private static void runPreloadPhase() throws InterruptedException {
		// start scheduler for preload phase and wait for completion
		SequentialScheduler scheduler;
		if (BenchFoundryConfigData.masterInstance)
			scheduler = new SequentialScheduler(false,
					BenchFoundryConfigData.traceFilenamePreload
							+ buildTempFileSuffix("master"), Phase.PRELOAD);
		else
			scheduler = new SequentialScheduler(false,
					BenchFoundryConfigData.traceFilenamePreload, Phase.PRELOAD);
		Thread schedulerThread = new Thread(scheduler);
		schedulerThread.setName("PRELOAD");
		schedulerThread.start();
		schedulerThread.join();
	}

	/**
	 * run on both master and slaves <br>
	 * 
	 * starts execution of the warm-up phase locally.
	 * 
	 * @return the thread executing the warm-up phase which should be
	 *         interrupted when starting the experiment phase.
	 */
	private static Thread startWarmupPhaseLocally() {
		SequentialScheduler scheduler = new SequentialScheduler(false,
				BenchFoundryConfigData.traceFilenameWarmup, Phase.WARMUP);
		Thread schedulerThread = new Thread(scheduler);
		schedulerThread.setName("WARMUP");
		schedulerThread.start();
		return schedulerThread;
	}

	/**
	 * only run on master <br>
	 * 
	 * proposes a start timestamp for the actual experiment to all slaves.
	 * Slaves may agree or propose a later timestamp (which will then start
	 * another communication round). The master then sends a final "go" for the
	 * specified start timestamp.
	 * 
	 */
	private static void determineExperimentStartTimestamp() {
		// agree on start timestamp for experiment phase; initial proposal for
		// in 30sec
		long expStart = System.currentTimeMillis() + 1000L * 30;
		if (!BenchFoundryConfigData.singleNode) {
			LOG.info("Starting agreement process for experiment start timestamp. Proposed value: "
					+ Time.format(expStart));
			expStart = MasterToSlaveCommunicator.getInstance()
					.proposeStartTimeToSlaves(Phase.EXPERIMENT, expStart);
			if (MasterToSlaveCommunicator.getInstance().commitStartTimestamp(
					Phase.EXPERIMENT, expStart)) {
				LOG.info("Agreement process concluded, experiment start timestamp is set to "
						+ Time.format(expStart));
			} else {
				LOG.error("Could not commit experiment start timestamp on slaves");
				System.exit(-1);
			}
		}
		Time.setBenchmarkStartTime(expStart);
	}

	/**
	 * run on both master and slaves <br>
	 * 
	 * @return the thread which is running the scheduler of the experiment
	 *         phase.
	 * 
	 */
	private static Thread startExperiment() {
		// start scheduler for benchmark phase
		Thread schedulerThread = null;
		if (BenchFoundryConfigData.closedWorkloadSchedulerInExperiment) {
			SequentialScheduler scheduler;
			if (BenchFoundryConfigData.masterInstance)
				scheduler = new SequentialScheduler(
						true,
						BenchFoundryConfigData.traceFilenameExperiment
								+ buildTempFileSuffix("master"),
						Phase.EXPERIMENT,
						true,
						BenchFoundryConfigData.closedWorkloadSchedulerThreadpoolSize);
			else
				scheduler = new SequentialScheduler(
						true,
						BenchFoundryConfigData.traceFilenameExperiment,
						Phase.EXPERIMENT,
						true,
						BenchFoundryConfigData.closedWorkloadSchedulerThreadpoolSize);
			schedulerThread = new Thread(scheduler);
		} else {
			Scheduler scheduler;
			if (BenchFoundryConfigData.masterInstance)
				scheduler = new Scheduler(true,
						BenchFoundryConfigData.traceFilenameExperiment
								+ buildTempFileSuffix("master"),
						Phase.EXPERIMENT);
			else
				scheduler = new Scheduler(true,
						BenchFoundryConfigData.traceFilenameExperiment,
						Phase.EXPERIMENT);
			schedulerThread = new Thread(scheduler);
		}
		schedulerThread.setName("EXPERIMENT");
		schedulerThread.start();
		return schedulerThread;
	}

	/**
	 * run on both master and slaves <br>
	 * 
	 * makes sure that all measurement results are persisted locally (or
	 * uploaded to a central data repository)
	 * 
	 */
	private static void finalizeMeasurementResults() {
		// FIXME add later: upload result data, e.g., to S3
		BenchFoundryConfigData.dbConnector.cleanup();
		MicroStatisticsCollector.printSchedulingDelayAggregates();
		MicroStatisticsCollector.exportMicroStatistics();

	}

	/**
	 * only run on master <br>
	 * 
	 * removes all traces of the benchmark run from the SUT, e.g., by dropping
	 * all tables etc.
	 */
	private static void cleanUpSUT() {
		LOG.info("Cleaning SUT.");
		BenchFoundryConfigData.dbConnector.cleanUpDatabase();
	}

	/**
	 * configures log4j
	 */
	private static void configureLog4j2() {
		LoggerContext context = (org.apache.logging.log4j.core.LoggerContext) LogManager
				.getContext(false);
		File file = new File("log4j2.xml");
		context.setConfigLocation(file.toURI());

	}

	/**
	 * 
	 * @param hostName
	 *            master or slavename
	 * @return the suffix for all temp files
	 */
	private static String buildTempFileSuffix(String hostName) {
		return "_" + hostName + ".tmp";
	}

}
