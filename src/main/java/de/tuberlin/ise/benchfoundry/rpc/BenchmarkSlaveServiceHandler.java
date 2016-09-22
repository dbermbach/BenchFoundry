/**
 * @author Akon Dey (akon.dey@gmail.com)
 *
 */
package de.tuberlin.ise.benchfoundry.rpc;

import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import de.tuberlin.ise.benchfoundry.util.BenchFoundryConfigData;
import de.tuberlin.ise.benchfoundry.util.IOUtils;
import de.tuberlin.ise.benchfoundry.util.Phase;
import de.tuberlin.ise.benchfoundry.util.PhaseManager;
import de.tuberlin.ise.benchfoundry.util.Time;
import de.tuberlin.ise.benchfoundry.rpc.BenchmarkSlaveService.Iface;

public class BenchmarkSlaveServiceHandler implements Iface {
	private static final Logger LOG = LogManager
			.getLogger(BenchmarkSlaveServiceHandler.class);

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.tuberlin.ise.benchfoundry.rpc.BenchmarkSlaveService.Iface#hello()
	 */
	@Override
	public String hello() throws TException {
		return "received hello from: "
				+ ManagementFactory.getRuntimeMXBean().getName();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * de.tuberlin.ise.benchfoundry.rpc.BenchmarkSlaveService.Iface#loadConfig
	 * (java .lang.String)
	 */
	@Override
	public void loadConfig(String config) throws TException {// FIXME
		throw new UnsupportedOperationException(
				"Should not be invoked for now.");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * de.tuberlin.ise.benchfoundry.rpc.BenchmarkSlaveService.Iface#loadTrace
	 * (java .lang.String)
	 */
	@Override
	public void loadTrace(String trace) throws TException {
		// FIXME
		throw new UnsupportedOperationException(
				"Should not be invoked for now.");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * de.tuberlin.ise.benchfoundry.rpc.BenchmarkSlaveService.Iface#proposeStartTime
	 * (long, java.lang.String)
	 */
	@Override
	public long proposeStartTimestamp(String phase, long timestamp)
			throws TException {
		// contract: return the proposed timestamp if acceptable or an
		// alternative (later) timestamp if not. Return -1 for a retry in X
		// seconds.
		// implementation: return the provided timestamp if already in warmup
		// phase and if the proposed timestamp is at least X seconds in the
		// future. Return -1 if an assessment is not yet possible.

		if (PhaseManager.getInstance().getCurrentPhase() == Phase.WARMUP) {
			if (timestamp - System.currentTimeMillis() >= BenchFoundryConfigData._minimumExperimentPreparationTimeOnSlavesInMs) {
				LOG.info("Received a proposed start time "
						+ Time.format(timestamp) + " for phase " + phase
						+ ", response: ACCEPT");
				return timestamp;
			} else {
				long newTime = System.currentTimeMillis()
						+ BenchFoundryConfigData._minimumExperimentPreparationTimeOnSlavesInMs;
				LOG.info("Received a proposed start time "
						+ Time.format(timestamp) + " for phase " + phase
						+ ", response: ALT. TIMESTAMP=" + Time.format(newTime));
				return newTime;
			}
		}
		LOG.info("Received a proposed start time " + Time.format(timestamp)
				+ " for phase " + phase
				+ ", response: ERROR since current phase="
				+ PhaseManager.getInstance().getCurrentPhase());
		return -1;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * de.tuberlin.ise.benchfoundry.rpc.BenchmarkSlaveService.Iface#commitStartTime
	 * (long, java.lang.String)
	 */
	@Override
	public void commitStartTimestamp(String phase, long timestamp)
			throws TException {
		// contract: if phase is experiment, simply set the start timestamp. for
		// preload phase, ignore timestamp and start it.
		// implementation: do just that, but also notify starter class
		// unclear: what do we do for other phases? FIXME

		if (Phase.valueOf(phase) == Phase.EXPERIMENT) {
			LOG.info("Accepted commit for EXPERIMENT start timestamp: "
					+ Time.format(timestamp));
			Time.setBenchmarkStartTime(timestamp);
			SlaveNotificationInbox.signalExperimentStartTimeIsSet();
		} else if (Phase.valueOf(phase) == Phase.PRELOAD) {
			LOG.info("Accepted commit for PRELOAD phase.");
			PhaseManager.getInstance().changePhaseTo(Phase.PRELOAD);
			SlaveNotificationInbox.signalProceedToPreload();
		} else {
			throw new UnsupportedOperationException(
					"commitStartTimestamp is only implemented for phases"
							+ " EXPERIMENT and PRELOAD. Received parameters phase="
							+ phase + ", timestamp=" + Time.format(timestamp));
		}

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * de.tuberlin.ise.benchfoundry.rpc.BenchmarkSlaveService.Iface#abortPhase
	 * (java .lang.String)
	 */
	@Override
	public void abortPhase(String phase) throws TException {// FIXME
		throw new UnsupportedOperationException(
				"Should not be invoked for now.");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * de.tuberlin.ise.benchfoundry.rpc.BenchmarkSlaveService.Iface#abortPhase
	 * (java .lang.String)
	 */
	@Override
	public void waitPhase(String phase) throws TException {// FIXME
		throw new UnsupportedOperationException(
				"Should not be invoked for now.");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * de.tuberlin.ise.benchfoundry.rpc.BenchmarkSlaveService.Iface#executeCommand
	 * (java.lang.String, java.util.List)
	 */
	@Override
	public String executeCommand(String command, List<String> arguments)// FIXME
			throws TException {
		switch (Commands.valueOf(command)) {
		case GET_HEALTH_INFO:
			return "SUCCESS"; // TODO: make this more meaningful
		default:
			return "FAILURE";
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * de.tuberlin.ise.benchfoundry.rpc.BenchmarkSlaveService.Iface#sendBlob
	 * (java .lang.String, java.nio.ByteBuffer)
	 */
	@Override
	public void sendBlob(String fileType, ByteBuffer blob) throws TException {// FIXME

		switch (FileType.valueOf(fileType)) {
		case PRELOAD_TRACE:
			IOUtils.writeFile(BenchFoundryConfigData.traceFilenamePreload, blob);
			SlaveNotificationInbox.signalPreloadTraceReceived();
			LOG.info("Received preload trace.");
			break;
		case WARMUP_TRACE:
			IOUtils.writeFile(BenchFoundryConfigData.traceFilenameWarmup, blob);
			SlaveNotificationInbox.signalWarmupTraceReceived();
			LOG.info("Received warmup trace.");
			break;
		case EXPERIMENT_TRACE:
			IOUtils.writeFile(BenchFoundryConfigData.traceFilenameExperiment,
					blob);
			SlaveNotificationInbox.signalExperimentTraceReceived();
			LOG.info("Received experiment trace.");
			break;
		case SCHEMA:
			IOUtils.writeFile(BenchFoundryConfigData.schemaFilename, blob);
			SlaveNotificationInbox.signalSchemaReceived();
			LOG.info("Received schema file.");
			break;
		case OP_LIST:
			IOUtils.writeFile(BenchFoundryConfigData.oplistFilename, blob);
			SlaveNotificationInbox.signalOplistReceived();
			LOG.info("Received oplist file.");
			break;
		case PARAM_LIST:
			IOUtils.writeFile(BenchFoundryConfigData.paramFilename, blob);
			SlaveNotificationInbox.signalParamlistReceived();
			LOG.info("Received param file.");
			break;
		case CUST_PARAM_LIST:
			IOUtils.writeFile(BenchFoundryConfigData.custparamFilename, blob);
			SlaveNotificationInbox.signalCustParamlistReceived();
			LOG.info("Received cust param file.");
			break;
		case CONFIG_DATA:
			byte[] array = new byte[blob.remaining()];
			blob.get(array);
			BenchFoundryConfigData.deserializeAndApplyConfigData(array);
			SlaveNotificationInbox.signalConfigDetailsReceived();
			LOG.info("Received configuration details.");
			break;
		case DB_CONFIG:
			IOUtils.writeFile(BenchFoundryConfigData.dbConnectorConfigFile,
					blob);
			SlaveNotificationInbox.signalDbConfigFileReceived();
			LOG.info("Received dbconnector config file.");
			break;
		default: {
			LOG.error("File type " + fileType + " is unknown.");
			throw new TException(new UnsupportedOperationException(
					"Unknown file type: " + fileType));
		}
		}
	}
}
