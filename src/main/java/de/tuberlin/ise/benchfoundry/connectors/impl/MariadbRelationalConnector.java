package de.tuberlin.ise.benchfoundry.connectors.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.tuberlin.ise.benchfoundry.connectors.IDbConnector;
import de.tuberlin.ise.benchfoundry.connectors.RelationalDbConnector;
import de.tuberlin.ise.benchfoundry.connectors.exceptions.PrepareTransactionException;
import de.tuberlin.ise.benchfoundry.connectors.transactions.RelationalOperation;
import de.tuberlin.ise.benchfoundry.connectors.transactions.RelationalTransaction;
import de.tuberlin.ise.benchfoundry.physicalschema.model.AbstractPhysicalSchema;
import de.tuberlin.ise.benchfoundry.physicalschema.model.relational.RelationalPhysicalSchema;
import de.tuberlin.ise.benchfoundry.physicalschema.model.relational.RelationalRequest;
import de.tuberlin.ise.benchfoundry.scheduling.BusinessTransaction;
import de.tuberlin.ise.benchfoundry.util.BenchFoundryConfigData;
import de.tuberlin.ise.benchfoundry.util.SelectiveLogEntry;
import de.tuberlin.ise.benchfoundry.util.Time;

/**
 * 
 * @author joernkuhlenkamp
 *
 */
public class MariadbRelationalConnector extends RelationalDbConnector {

	private static final Logger LOG = LogManager
			.getLogger(MariadbRelationalConnector.class);

	public final static String URI_KEY = "mariadb.uri";
	public final static String USER_KEY = "mariadb.user";
	public final static String PASS_KEY = "mariadb.password";
	public final static String DB_KEY = "mariadb.db";

	private String uri;
	private String user;
	private String pass;
	private String databaseName;

	/** Connection to Mariadb */
	private Connection dbConnection;
	/**
	 * Lookup of {@link RelationalTransactions} by ID of
	 * {@link BusinessTransaction}
	 */
	private final Map<String, RelationalTransaction> transactions = new ConcurrentHashMap<>();

	public MariadbRelationalConnector() {
		Properties prop = new Properties();
		InputStream propIn;
		try {
			propIn = new FileInputStream(
					BenchFoundryConfigData.dbConnectorConfigFile);

			prop.load(propIn);

			if (!prop.containsKey(URI_KEY)) {
				throw new IllegalArgumentException(
						"Missing property with key '"
								+ URI_KEY
								+ "' in MariaDB configuration file '"
								+ BenchFoundryConfigData.dbConnectorConfigFile
								+ "'.");
			}
			if (!prop.containsKey(USER_KEY)) {
				throw new IllegalArgumentException(
						"Missing property with key '"
								+ USER_KEY
								+ "' in MariaDB configuration file '"
								+ BenchFoundryConfigData.dbConnectorConfigFile
								+ "'.");
			}
			if (!prop.containsKey(PASS_KEY)) {
				throw new IllegalArgumentException(
						"Missing property with key '"
								+ PASS_KEY
								+ "' in MariaDB configuration file '"
								+ BenchFoundryConfigData.dbConnectorConfigFile
								+ "'.");
			}
			if (!prop.containsKey(DB_KEY)) {
				throw new IllegalArgumentException(
						"Missing property with key '"
								+ DB_KEY
								+ "' in MariaDB configuration file '"
								+ BenchFoundryConfigData.dbConnectorConfigFile
								+ "'.");
			}

			uri = prop.getProperty(URI_KEY);
			user = prop.getProperty(USER_KEY, "root");
			pass = prop.getProperty(PASS_KEY, "root");
			databaseName = prop.getProperty(DB_KEY, "test");

			if (uri == "" || uri == null)
				throw new IllegalArgumentException("Illegal value for key '"
						+ URI_KEY + "' in MariaDB configuration file '"
						+ BenchFoundryConfigData.dbConnectorConfigFile
						+ "'.");

		} catch (Exception e) {
			LOG.fatal("Cannot run BenchFoundry without dbconnector config file. Reading config file "
					+ BenchFoundryConfigData.dbConnectorConfigFile
					+ " failed: " + e.getMessage());
			e.printStackTrace();
			System.exit(-1);

		}

	}

	/**
	 * private constructor for deserialized dbconnector instances
	 * 
	 * @param typeAndUri
	 * @param user
	 * @param password
	 * @param databaseName
	 * @throws SQLException
	 */
	private MariadbRelationalConnector(String typeAndUri, String user,
			String password, String databaseName) throws SQLException {
		dbConnection = DriverManager.getConnection(typeAndUri, user, password);
		this.uri = typeAndUri;
		this.user = user;
		this.pass = password;
		this.databaseName = databaseName;
	}

	@Override
	public void executeBusinessTransaction(long processId, long transactionId,
			SelectiveLogEntry log) throws SQLException {
		if (SelectiveLogEntry.doDetailledLogging)
			log.log(this, "starting execution for txId=" + transactionId);
		Statement stmt = dbConnection.createStatement();
		RelationalTransaction t = transactions.remove(processId + "."
				+ transactionId);

		List<List<String>> responsePayload = new ArrayList<>();

		if (t.doMeasurements())
			t.setTransactionStart(Time.now());
		if (t.hasTransactionalGurantees())
			stmt.executeQuery("START TRANSACTION;");
		for (RelationalOperation o : t.getOperations()) {
			long e1 = -1, e2 = -1;

			// TODO SELECT and UPDATE operations provide a ResultSet or an int
			// as result. We should add a better case handling.
			if (o.getOperationStmt().contains("SELECT")) { // FIXME add JOIN
				if (t.doMeasurements())
					e1 = Time.now();
				ResultSet rs = stmt.executeQuery(o.getOperationStmt());
				if (t.doMeasurements())
					e2 = Time.now();
				ResultSetMetaData md = rs.getMetaData();

				int columnCount = md.getColumnCount();
				List<String> columnNames = new ArrayList<>();
				for (int i = 1; i <= columnCount; i++) {
					columnNames.add(md.getColumnName(i));
				}
				responsePayload.add(columnNames);
				while (rs.next()) {
					List<String> rowFields = new ArrayList<>();
					for (int i = 1; i <= columnCount; i++) {
						rowFields.add(rs.getString(md.getColumnName(i)));
					}
					responsePayload.add(rowFields);
				}
				o.setResponsePayload(responsePayload);
			} else if (o.getOperationStmt().contains("INSERT")
					|| o.getOperationStmt().contains("UPDATE")) {
				if (t.doMeasurements())
					e1 = Time.now();
				int rs = stmt.executeUpdate(o.getOperationStmt());
				if (t.doMeasurements())
					e2 = Time.now();
				responsePayload.add(Arrays.asList("Query statement",
						"Return value"));
				responsePayload.add(Arrays.asList(o.getOperationStmt(),
						String.valueOf(rs)));
				o.setResponsePayload(responsePayload);
			}

			if (t.doMeasurements())
				o.setOperationStart(e1);
			o.setOperationEnd(e2);
			// LOG.debug("[pid:" +t.getProcessId()+
			// ", tid:"+t.getTransactionId()+ ", oid:"
			// +o.getOperationId()+"] e1-e2:" +String.valueOf(e2-e1)+ ", e1-e3:"
			// +String.valueOf(e3-e1)+ ", e2-e3:" +String.valueOf(e3-e2));
		}
		if (t.hasTransactionalGurantees())
			stmt.executeQuery("COMMIT;");
		if (t.doMeasurements())
			t.setTransactionEnd(Time.now());
		if (SelectiveLogEntry.doDetailledLogging)
			log.log(this, "end of execution.");
		if (t.doMeasurements())
			t.log();
		if (SelectiveLogEntry.doDetailledLogging)
			log.log(this,
					"transaction results have been sent to the result logger.");
		// FIXME explicitly set resulttype for operations and transactions!

	}

	@Override
	protected void prepareRelationalTransaction(long processId,
			long transactionId, List<RelationalRequest> operations,
			List<Integer> businessOperationIds, List<List<String>> params,
			boolean doMeasurements, SelectiveLogEntry log)
			throws PrepareTransactionException {
		// LOG.debug("Preparing RelationalTransaction - processId:"+processId+",
		// transactionId:"+transactionId);
		boolean doLog = SelectiveLogEntry.doDetailledLogging;
		// LOG.debug("in prepareRelationalTransaction for processId=" +
		// processId
		// + " (params=" + params + ")");
		if (doLog)
			log.log(this, "Preparing transaction txId=" + transactionId);
		RelationalTransaction t;
		if (operations.size() != params.size())
			throw new PrepareTransactionException("There were "
					+ operations.size() + "operations but params for "
					+ params.size() + " operations.");

		if (operations.size() > 1) {
			t = new RelationalTransaction(processId, transactionId, true,
					doMeasurements);
		} else {
			t = new RelationalTransaction(processId, transactionId, false,
					doMeasurements);
		}
		if (doLog)
			log.log(this, "RelationalTransaction object created with "
					+ operations.size() + " operations.");
		// LOG.debug("in prepareRelationalTransaction got object for processId="
		// + processId + " with " + operations.size() + " operations.");
		for (int i = 0; i < operations.size(); i++) {
			String queryTemp = operations.get(i).getSqlQuery();
			List<String> paramTemp = params.get(i);
			if (doLog) {
				log.logLocalVariable(
						"query (" + (i + 1) + "/" + operations.size() + ")",
						queryTemp);
				log.logLocalVariable(
						"params (" + (i + 1) + "/" + operations.size() + ")",
						paramTemp);
			}
			// LOG.debug("Got data: " + queryTemp + " and " + paramTemp);
			String stmt = reassembleSQLRequest(queryTemp, paramTemp, log);
			if (doLog)
				log.logLocalVariable("resulting statement (" + (i + 1) + "/"
						+ operations.size() + ")", stmt);

			// String stmt = createSqlRequest(operations.get(i).getSqlQuery(),
			// params.get(i));
			// LOG.debug("processId=" + processId + ": got " + i
			// + "st statement out of " + operations.size() + " - " + stmt);

			t.addOperation(new RelationalOperation(t, businessOperationIds
					.get(i), stmt, operations.get(i).getLogicalQueryId()));
			// LOG.debug("done adding op " + i + " for processId=" + processId);
			if (doLog)
				log.log(this, "added relational operation (" + (i + 1) + "/"
						+ operations.size() + ")");
		}
		// LOG.debug("in prepareRelationalTransaction invoking put for
		// processId="
		// + processId);
		transactions.put(processId + "." + transactionId, t);
		if (doLog)
			log.log(this, "added transaction txid=" + transactionId
					+ " to map.");
	}

	@Override
	public void init() {
		try {
			dbConnection = DriverManager.getConnection(uri + databaseName,
					user, pass);
		} catch (SQLException e) {
			LOG.error("Could not open connection to database:" + e.getMessage());
			e.printStackTrace();
			System.exit(-1);
		}

		LOG.info("Successfully established connection to database '"
				+ databaseName + "'.");
	}

	@Override
	public void cleanup() {
		try {
			dbConnection.close();
		} catch (SQLException e) {
			LOG.error("Error while closing connection to database:"
					+ e.getMessage());
			e.printStackTrace();
		}
	}

	private void setupDB() throws SQLException {
		dbConnection = DriverManager.getConnection(uri, user, pass);
		LOG.info("Successfully established connection to MariaDB at endpoint '"
				+ uri + "'.");
		Statement stmt = dbConnection.createStatement();
		stmt.executeQuery("DROP DATABASE IF EXISTS " + databaseName + ";");
		stmt.executeQuery("CREATE DATABASE " + databaseName + ";");
		LOG.info("Successfully created database '" + databaseName + "'.");
		init();
	}

	private void setupSchema(RelationalPhysicalSchema schema)
			throws SQLException {
		Map<Integer, String> queries = schema.getTableCreationStatements();

		if (dbConnection == null)
			throw new NullPointerException(
					"No connection to database established.");

		for (String query : queries.values()) {
			Statement stmt = dbConnection.createStatement();
			stmt.executeQuery(query);
			// LOG.info(stmt);
		}

		LOG.info("Successfully created tables.");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * de.tuberlin.ise.benchfoundry.connectors.IDbConnector#setupPhysicalSchema
	 * (de
	 * .tuberlin.ise.benchfoundry.physicalschema.model.AbstractPhysicalSchema)
	 */
	@Override
	public void setupPhysicalSchema(AbstractPhysicalSchema schema) {
		try {
			setupDB();
		} catch (SQLException e) {
			new IllegalStateException("Failed to create database "
					+ databaseName + " for MariadDB.");
		}
		try {
			setupSchema((RelationalPhysicalSchema) schema);
		} catch (SQLException e) {
			LOG.error("Failed to create tables in database: " + e.getMessage());
			throw new IllegalStateException(
					"Failed to create tables in database " + databaseName
							+ " for MariadDB.");
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * de.tuberlin.ise.benchfoundry.connectors.IDbConnector#cleanUpDatabase()
	 */
	@Override
	public void cleanUpDatabase() {
		// TODO Auto-generated method stub
		// throw new UnsupportedOperationException();
		LOG.warn("cleanUpDatabase() is not implemented yet");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * de.tuberlin.ise.benchfoundry.connectors.IDbConnector#serializeConnector()
	 */
	@Override
	public byte[] serializeConnector() {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try {
			ObjectOutputStream oos = new ObjectOutputStream(baos);
			oos.writeObject(this.uri);
			oos.writeObject(this.user);
			oos.writeObject(this.pass);
			oos.writeObject(this.databaseName);
			oos.close();
			return baos.toByteArray();

		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * de.tuberlin.ise.benchfoundry.connectors.IDbConnector#deserializeConnector
	 * ( byte[])
	 */
	@Override
	public IDbConnector deserializeConnector(byte[] serializedConnector) {
		IDbConnector result = null;
		try {
			ObjectInputStream ois = new ObjectInputStream(
					new ByteArrayInputStream(serializedConnector));
			String uri = (String) ois.readObject();
			String user = (String) ois.readObject();
			String pass = (String) ois.readObject();
			String databaseName = (String) ois.readObject();
			result = new MariadbRelationalConnector(uri, user, pass,
					databaseName);
			ois.close();
		} catch (Exception e) {
			LOG.error("Could not deserialize object: " + e.getMessage(), e);
			e.printStackTrace();
		}
		return result;
	}

}
