/**
 * 
 */
package de.tuberlin.ise.benchfoundry.rpc;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import de.tuberlin.ise.benchfoundry.util.BenchFoundryConfigData;
import de.tuberlin.ise.benchfoundry.util.Phase;
import de.tuberlin.ise.benchfoundry.util.Time;
import de.tuberlin.ise.benchfoundry.rpc.BenchmarkSlaveService;

/**
 * @author Dave
 *
 */
public class MasterToSlaveCommunicator {

	private static final Logger LOG = LogManager
			.getLogger(MasterToSlaveCommunicator.class);

	/** singleton instance */
	private final static MasterToSlaveCommunicator instance = new MasterToSlaveCommunicator();

	/** holds all active connections' client stubs */
	private final Map<String, BenchmarkSlaveService.Client> connections = new HashMap<>();

	/** holds all active connections' TTransport objects for later closing */
	private final Map<String, TTransport> connTransports = new HashMap<>();

	/** will be set to true when all connections have been opened */
	private boolean connectionsOpened = false;

	private MasterToSlaveCommunicator() {
	}

	/**
	 * 
	 * @return the singleton instance
	 */
	public static MasterToSlaveCommunicator getInstance() {
		return instance;
	}

	/**
	 * Tries to create thrift connections to all known slave instances. It will
	 * print an error message if any of those connections fails.
	 * 
	 * @return true if all connections could be created successfully, false
	 *         otherwise.
	 */
	public boolean openConnections() {
		for (RPCHost host : BenchFoundryConfigData.slaves.values()) {
			try {
				TTransport transport;

				transport = new TSocket(host.host, host.port);
				transport.open();

				TProtocol protocol = new TBinaryProtocol(transport);
				BenchmarkSlaveService.Client client = new BenchmarkSlaveService.Client(
						protocol);

				client.hello();

				connections.put(host.name, client);
				connTransports.put(host.name, transport);
				LOG.info("Connection to slave " + host + " is now open.");

			} catch (TException x) {
				LOG.error("Could not open connection to slave.");
				x.printStackTrace();
				return false;
			}
		}
		return connectionsOpened = true;
	}

	/**
	 * Closes all existing slave connections
	 */
	public void closeConnections() {
		for (RPCHost host : BenchFoundryConfigData.slaves.values()) {
			connTransports.get(host.name).close();
			connTransports.remove(host.name);
			connections.remove(host.name);
			LOG.info("Connection to slave " + host + " is now closed.");
		}
		connectionsOpened = false;
	}

	/**
	 * Sends a file to a specific slave
	 * 
	 * @param slaveName
	 *            id of the recipient
	 * @param fileType
	 *            type of the file (so that recipient can use it properly)
	 * @param blob
	 *            binary content of the file
	 * @return true if successful
	 */
	private boolean sendConfigFileBlob(String slaveName, FileType fileType,
			byte[] blob) {
		if (!connectionsOpened) {
			LOG.error("Connections are not open.");
			return false;
		}
		LOG.debug("sending " + fileType + " to slave " + slaveName
				+ ", size in bytes=" + blob.length);
		BenchmarkSlaveService.Client client = connections.get(slaveName);
		if (client == null)
			return false;

		try {
			client.sendBlob(fileType.name(), ByteBuffer.wrap(blob));
		} catch (Exception e) {
			LOG.error("sendConfigFileBlob(" + fileType + ", ...) to slave "
					+ slaveName + " failed with: " + e.getMessage());
			return false;
		}
		return true;
	}

	/**
	 * Proposes the specified start timestamp for the specified phase to all
	 * slaves.
	 * 
	 * @param phase
	 * @param startTimestamp
	 * @return -1 if an error occured; a timestamp where all slaves will be
	 *         ready to start the phase.
	 */
	public long proposeStartTimeToSlaves(Phase phase, long startTimestamp) {
		long latestStart = startTimestamp;
		boolean retry = true;
		while (retry) {
			retry = false;
			if (!connectionsOpened) {
				LOG.error("Connections are not open.");
				return -1;
			}
			for (String slaveName : connections.keySet()) {
				BenchmarkSlaveService.Client client = connections
						.get(slaveName);
				long response;
				try {
					// contract: will return the proposed timestamp if
					// acceptable or an
					// alternative (later) timestamp if not. Will return -1 if
					// not yet ready to decide.
					response = client.proposeStartTimestamp(phase.name(),
							startTimestamp);
					// LOG.debug("proposed timestamp "
					// + Time.format(startTimestamp) + " for phase "
					// + phase + " to slave " + slaveName + ", response:"
					// + response + "(" + Time.format(response) + ")");
				} catch (TException e) {
					LOG.info("Slave " + slaveName
							+ " failed to accept timestamp for starting phase "
							+ phase + "with error: " + e.getMessage());
					e.printStackTrace();
					return -1;
				}
				// if any slave is not ready, retry in X seconds
				if (response == -1) {
					LOG.info("Slave "
							+ slaveName
							+ " is not ready for accepting a start time. Retrying in "+BenchFoundryConfigData._startTimestampAgreementRetryIntervalInSeconds+" seconds.");
					retry = true;
					startTimestamp += BenchFoundryConfigData._startTimestampAgreementRetryIntervalInSeconds*1000;
					try {
						Thread.sleep(BenchFoundryConfigData._startTimestampAgreementRetryIntervalInSeconds*1000);
					} catch (InterruptedException e) {
						LOG.error(
								"Thread was interrupted while waiting: "
										+ e.getMessage(), e);
						e.printStackTrace();
					}
					break;
				}
				if (response != startTimestamp)
					LOG.info("Slave " + slaveName + " proposes to start phase "
							+ phase + " "
							+ (response - startTimestamp / 1000.0)
							+ " seconds later.");
				if (response > latestStart)
					latestStart = response;
			}
		}
		return latestStart;
	}

	/**
	 * Sends the definite start timestamp for a specific phase to all slaves.
	 * <b><b>Attention: use proposeStartTimestamp first to make sure all slaves
	 * are able to execute at that time!
	 * 
	 * 
	 * @param phase
	 * @param startTimestamp
	 * @return false if anything went wrong, true otherwise
	 */
	public boolean commitStartTimestamp(Phase phase, long startTimestamp) {
		if (!connectionsOpened) {
			LOG.error("Connections are not open.");
			return false;
		}
		for (String slaveName : connections.keySet()) {
			BenchmarkSlaveService.Client client = connections.get(slaveName);
			try {
				// contract: if phase is experiment, slave will set the start
				// timestamp. for
				// preload phase, slave will ignore timestamp and start phase.
				client.commitStartTimestamp(phase.name(), startTimestamp);
				LOG.info("Slave " + slaveName + " commits to starting phase "
						+ phase + " at " + Time.format(startTimestamp));
			} catch (TException e) {
				LOG.error("Slave " + slaveName
						+ " failed to commit to starting phase " + phase
						+ " at " + Time.format(startTimestamp)
						+ " due to the following error: " + e.getMessage());
				e.printStackTrace();
				return false;
			}
		}
		return true;
	}

	/**
	 * Executes a generic command to the specified slave and returns its
	 * response
	 * 
	 * 
	 * @param slaveName
	 * @param command
	 * @param param
	 *            optional field, specifies additional parameters for the
	 *            command
	 * @return null if an error occured or the slave's response otherwise
	 */
	public String executeCommand(String slaveName, Commands command,
			List<String> params) {
		if (!connectionsOpened) {
			LOG.error("Connections are not open.");
			return null;
		}
		BenchmarkSlaveService.Client client = connections.get(slaveName);

		try {
			String result = client.executeCommand(command.name(), params);
			LOG.info("Slave " + slaveName + " executed command "
					+ command.name() + " with parameters " + params
					+ "; returned value: " + result);
			return result;
		} catch (TException e) {
			LOG.error("Slave " + slaveName + " failed to execute command "
					+ command.name() + " with parameters " + params
					+ "; error was " + e.getMessage());
			return null;
		}
	}

	/**
	 * Sends the specified file content as preload trace to the specified slave
	 * 
	 * @param slaveName
	 * @param fileContent
	 * @return true if successful
	 */
	public boolean sendPreloadTrace(String slaveName, byte[] fileContent) {
		return sendConfigFileBlob(slaveName, FileType.PRELOAD_TRACE,
				fileContent);
	}

	/**
	 * sends the specified file content as warmup trace to the specified slave
	 * 
	 * @param slaveName
	 * @param fileContent
	 * @return true if successful
	 */
	public boolean sendWarmupTrace(String slaveName, byte[] fileContent) {
		return sendConfigFileBlob(slaveName, FileType.WARMUP_TRACE, fileContent);
	}

	/**
	 * Sends the specified file content as experiment trace to the specified
	 * slave
	 * 
	 * @param slaveName
	 * @param fileContent
	 * @return true if successful
	 */
	public boolean sendExperimentTrace(String slaveName, byte[] fileContent) {
		return sendConfigFileBlob(slaveName, FileType.EXPERIMENT_TRACE,
				fileContent);
	}

	/**
	 * Sends the specified configuration details to the specified slave
	 * 
	 * @param slaveName
	 * @param fileContent
	 * @return true if successful
	 */
	public boolean sendConfigDetails(String slaveName, byte[] fileContent) {
		return sendConfigFileBlob(slaveName, FileType.CONFIG_DATA, fileContent);
	}

	/**
	 * Broadcasts the schema file to all slaves
	 * 
	 * @param fileContent
	 * @return
	 */
	public boolean broadcastSchema(byte[] fileContent) {
		for (String slave : connections.keySet()) {
			if (!sendConfigFileBlob(slave, FileType.SCHEMA, fileContent))
				return false;
		}
		return true;
	}

	/**
	 * Broadcasts the op list file to all slaves
	 * 
	 * @param fileContent
	 * @return true if sent to all slaves successfully
	 */
	public boolean broadcastOpList(byte[] fileContent) {
		for (String slave : connections.keySet()) {
			if (!sendConfigFileBlob(slave, FileType.OP_LIST, fileContent))
				return false;
		}
		return true;
	}

	/**
	 * Broadcasts the param list file to all slaves
	 * 
	 * @param fileContent
	 * @return true if sent to all slaves successfully
	 */
	public boolean broadcastParamList(byte[] fileContent) {
		for (String slave : connections.keySet()) {
			if (!sendConfigFileBlob(slave, FileType.PARAM_LIST, fileContent))
				return false;
		}
		return true;
	}

	/**
	 * Broadcasts the cust param list file to all slaves
	 * 
	 * @param fileContent
	 * @return true if sent to all slaves successfully
	 */
	public boolean broadcastCustParamList(byte[] fileContent) {
		for (String slave : connections.keySet()) {
			if (!sendConfigFileBlob(slave, FileType.CUST_PARAM_LIST,
					fileContent))
				return false;
		}
		return true;
	}

	/**
	 * Broadcasts the dbconnector config file to all slaves
	 * 
	 * @param fileContent
	 * @return true if sent to all slaves successfully
	 */
	public boolean broadcastDbConnectorConfigFile(byte[] fileContent) {
		for (String slave : connections.keySet()) {
			if (!sendConfigFileBlob(slave, FileType.DB_CONFIG,
					fileContent))
				return false;
		}
		return true;
	}
	
}
