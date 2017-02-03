package de.tuberlin.ise.benchfoundry.connectors;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.tuberlin.ise.benchfoundry.connectors.exceptions.PrepareTransactionException;
import de.tuberlin.ise.benchfoundry.connectors.transactions.RelationalOperation;
import de.tuberlin.ise.benchfoundry.connectors.transactions.RelationalTransaction;
import de.tuberlin.ise.benchfoundry.physicalschema.model.AbstractPhysicalSchema;
import de.tuberlin.ise.benchfoundry.physicalschema.model.AbstractRequest;
import de.tuberlin.ise.benchfoundry.physicalschema.model.relational.RelationalPhysicalSchema;
import de.tuberlin.ise.benchfoundry.physicalschema.model.relational.RelationalRequest;
import de.tuberlin.ise.benchfoundry.scheduling.BusinessTransaction;
import de.tuberlin.ise.benchfoundry.util.BenchFoundryConfigData;
import de.tuberlin.ise.benchfoundry.util.SelectiveLogEntry;
import de.tuberlin.ise.benchfoundry.util.Time;

/**
 * This class is a generic JDBC-based connector for relational database systems.
 * 
 * <br>
 * Note: Non-abstract subclasses must define a no-argument constructor.
 * 
 * 
 * @author joernkuhlenkamp, dbermbach
 *
 */
public abstract class RelationalDbConnector implements IDbConnector {

	/** Connection to the relational database */
	private Connection dbConnection;

	private static final Logger LOG = LogManager
			.getLogger(RelationalDbConnector.class);

	/** URI where the actual database can be reached */
	private String uri;

	/** user name for the database system */
	private String user;

	/** password for the database system */
	private String pass;

	/** name of the test database */
	private String databaseName;

	/**
	 * where the bare database system without a concrete database can be reached
	 */
	private String systemURI;

	/**
	 * Lookup of {@link RelationalTransactions} by ID of
	 * {@link BusinessTransaction}
	 */
	private final Map<String, RelationalTransaction> transactions = new ConcurrentHashMap<>();

	/**
	 * 
	 * 
	 * default constructor
	 * 
	 */
	public RelationalDbConnector() {
		super();
		// will be followed by a call to applySerializedData() if on slaves
		if (BenchFoundryConfigData.masterInstance) {
			this.databaseName = getDatabaseName();
			this.uri = getDatabaseURI();
			this.user = getDatabaseUsername();
			this.pass = getDatabasePassword();
			this.systemURI = getDatabaseSystemURI();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * de.tuberlin.ise.benchfoundry.connectors.IDbConnector#prepareTransaction
	 * (long, long, java.util.List, java.util.List, java.util.List,
	 * java.util.List, boolean,
	 * de.tuberlin.ise.benchfoundry.util.SelectiveLogEntry)
	 */
	@Override
	public void prepareTransaction(long processId, long transactionId,
			List<List<? extends AbstractRequest>> operations,
			List<Integer> businessOperationIds, List<List<String>> params,
			List<List<String>> custParams, boolean doMeasurements,
			SelectiveLogEntry log) throws PrepareTransactionException {
		List<RelationalRequest> ops = new ArrayList<>();
		for (List<? extends AbstractRequest> l : operations) {
			if (l.size() > 1)
				throw new PrepareTransactionException(
						"More than one request in operation: " + l);
			for (AbstractRequest r : l) {
				ops.add((RelationalRequest) r);
			}
		}
		if (SelectiveLogEntry.doDetailledLogging)
			log.log(RelationalDbConnector.class,
					"All requests have been typecast.");
		prepareRelationalTransaction(processId, transactionId, ops,
				businessOperationIds, params, doMeasurements, log);
	}

	/**
	 * reassembles query strings and parameters for all queries of a
	 * transactions; enqueues completed transactions in field transactions.
	 * 
	 * 
	 * @param processId
	 * @param transactionId
	 * @param operations
	 * @param businessOperationIds
	 * @param params
	 * @param doMeasurements
	 * @param log
	 * @throws PrepareTransactionException
	 *             when the number of parameters and wildcards for a query do
	 *             not match
	 */
	protected void prepareRelationalTransaction(long processId,
			long transactionId, List<RelationalRequest> operations,
			List<Integer> businessOperationIds, List<List<String>> params,
			boolean doMeasurements, SelectiveLogEntry log)
			throws PrepareTransactionException {
		boolean doLog = SelectiveLogEntry.doDetailledLogging;
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
			String stmt = reassembleSQLRequest(queryTemp, paramTemp, log);
			if (doLog)
				log.logLocalVariable("resulting statement (" + (i + 1) + "/"
						+ operations.size() + ")", stmt);
			t.addOperation(new RelationalOperation(t, businessOperationIds
					.get(i), stmt, operations.get(i).getLogicalQueryId()));
			if (doLog)
				log.log(this, "added relational operation (" + (i + 1) + "/"
						+ operations.size() + ")");
		}
		transactions.put(processId + "." + transactionId, t);
		if (doLog)
			log.log(this, "added transaction txid=" + transactionId
					+ " to map.");
	}

	/**
	 * replaces all "?" wildcards with the parameters from "params" in the order
	 * specified in that method.
	 * 
	 * @param req
	 *            a SQL String with "?" wildcards for parameter values
	 * @param params
	 *            a sequence of parameters
	 * @param log
	 * @return a SQL String without wildcards
	 * @throws PrepareTransactionException
	 *             if the number of wildcards in req does not equal the number
	 *             of entries in params
	 */
	private String reassembleSQLRequest(String req, List<String> params,
			SelectiveLogEntry log) throws PrepareTransactionException {
		// Determine number of "?" occurrences in SQL string of {@link
		// BusinessTransaction}

		int paramNumb = 0;
		for (char c : req.toCharArray())
			if (c == '?')
				paramNumb++;
		// LOG.debug("req=" + req + ", params=" + params + ", paramNumb="
		// + paramNumb);
		if (SelectiveLogEntry.doDetailledLogging) {
			log.logLocalVariable("req", req);
			log.logLocalVariable("params", params);
			log.logLocalVariable("paramNumb", paramNumb);
		}
		if (paramNumb != params.size()) {
			if (SelectiveLogEntry.doDetailledLogging) {
				log.log(this, "Have " + params.size() + " params, should be "
						+ paramNumb + ":");
				for (String s : params)
					log.log(this, "param entry:" + s);
			}
			throw new PrepareTransactionException("Have " + params.size()
					+ " params, should be " + paramNumb + "(\nquery=" + req
					+ "\nparams=" + params + ")");
		}

		StringBuilder sb = new StringBuilder(req);
		for (int i = 0; i < paramNumb; i++) {
			int index = sb.indexOf("?");
			sb.replace(index, index + 1, params.get(i));
			// log.logLocalVariable("sb", sb);

		}
		return sb.toString();
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
		if (!BenchFoundryConfigData.masterInstance)
			return;
		if (systemURI == null)
			this.systemURI = getDatabaseSystemURI();
		if (systemURI != null) {
			// create database
			try {
				dbConnection = DriverManager.getConnection(systemURI, user,
						pass);
				LOG.info("Successfully established connection to database system at endpoint '"
						+ systemURI + "'.");
				Statement stmt = dbConnection.createStatement();
				stmt.executeQuery("DROP DATABASE IF EXISTS " + databaseName
						+ ";");
				stmt.executeQuery("CREATE DATABASE " + databaseName + ";");
				LOG.info("Successfully created database '" + databaseName
						+ "'.");
			} catch (SQLException e) {
				LOG.error("Failed to create database " + databaseName
						+ " at endpoint " + systemURI + ":" + e.getMessage());
				throw new IllegalStateException("Failed to create database "
						+ databaseName + " at endpoint " + systemURI + ".", e);
			}
		}
		// recreate connection to new database
		init();
		LOG.info("Successfully established connection to database '"
				+ databaseName + "'.");
		// create tables
		try {
			Collection<String> queries = ((RelationalPhysicalSchema) schema)
					.getTableCreationStatements().values();
			if (queries == null || queries.size() == 0)
				throw new SQLException(
						"Provided schema did not contain queries.");

			if (dbConnection == null)
				throw new IllegalStateException(
						"No connection to database established.");

			for (String query : queries) {
				LOG.info("Creating table with query: "+query);
				Statement stmt = dbConnection.createStatement();
				stmt.executeUpdate(query);
				// LOG.info(stmt);
			}
			LOG.info("Successfully created tables.");
		} catch (SQLException e) {
			LOG.error("Failed to create tables in database: " + e.getMessage());
			throw new IllegalStateException(
					"Failed to create tables in database " + databaseName + ".",
					e);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.tuberlin.ise.benchfoundry.connectors.IDbConnector#init()
	 */
	@Override
	public void init() {
		// recreate connection to database
		try {
			dbConnection = DriverManager.getConnection(uri, user, pass);
		} catch (SQLException e) {
			LOG.error("Could not connect to database: " + e.getMessage());
			throw new IllegalStateException("Could not connect to database "
					+ databaseName + ".", e);
		}
		LOG.info("Successfully established connection to database '"
				+ databaseName + "'.");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.tuberlin.ise.benchfoundry.connectors.IDbConnector#
	 * executeBusinessTransaction(long, long,
	 * de.tuberlin.ise.benchfoundry.util.SelectiveLogEntry)
	 */
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
			if (o.getOperationStmt().contains("SELECT")
					|| o.getOperationStmt().contains("JOIN")) {
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
			} else if (o.getOperationStmt().contains("INSERT") // FIXME add
																// DELETE?
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * de.tuberlin.ise.benchfoundry.connectors.IDbConnector#serializeConnectorState
	 * ()
	 */
	@Override
	public byte[] serializeConnectorState() {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try {
			ObjectOutputStream oos = new ObjectOutputStream(baos);
			oos.writeObject(this.uri);
			oos.writeObject(this.user);
			oos.writeObject(this.pass);
			oos.writeObject(this.databaseName);
			byte[] implSpecific = getSerializedImplSpecificData();
			if (implSpecific == null)
				implSpecific = "N/A".getBytes();
			oos.writeObject(implSpecific);
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
	 * @see de.tuberlin.ise.benchfoundry.connectors.IDbConnector#
	 * applySerializedConnectorState(byte[])
	 */
	@Override
	public void applySerializedConnectorState(byte[] serializedState) {
		try {
			ObjectInputStream ois = new ObjectInputStream(
					new ByteArrayInputStream(serializedState));
			this.uri = (String) ois.readObject();
			this.user = (String) ois.readObject();
			this.pass = (String) ois.readObject();
			this.databaseName = (String) ois.readObject();
			byte[] implSpecific = (byte[]) ois.readObject();
			if (!new String(implSpecific).equals("N/A"))
				applySerializedImplSpecificData(implSpecific);
			ois.close();
			init(); // open dbconnection
		} catch (Exception e) {
			LOG.error(
					"Could not deserialize object or open connection to database: "
							+ e.getMessage(), e);
			e.printStackTrace();
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
		if (!BenchFoundryConfigData.masterInstance || systemURI == null)
			return;
		try {
			dbConnection = DriverManager.getConnection(systemURI, user, pass);
			LOG.info("Successfully established connection to bare database system at endpoint '"
					+ uri + "'.");
			Statement stmt = dbConnection.createStatement();
			stmt.executeQuery("DROP DATABASE IF EXISTS " + databaseName + ";");
			LOG.info("Successfully dropped database '" + databaseName + "'.");
		} catch (SQLException e) {
			LOG.error("Failed to drop database: " + e.getMessage(), e);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.tuberlin.ise.benchfoundry.connectors.IDbConnector#cleanup()
	 */
	@Override
	public void cleanup() {
		try {
			dbConnection.close();
		} catch (SQLException e) {
			LOG.error(
					"Error while closing connection to database:"
							+ e.getMessage(), e);
		}
	}

	/**
	 * @return the dbConnection
	 */
	protected Connection getDbConnection() {
		return this.dbConnection;
	}

	/**
	 * 
	 * 
	 * @return the full URI where the actual database can be reached over JDBC
	 */
	protected abstract String getDatabaseURI();

	/**
	 * 
	 * @return the full URI where the database system can be reached over JDBC.
	 *         The returned value should not include a database name as the
	 *         connector, if this method returns anything but null, will try to
	 *         create a database with the name provided by getDatabaseName()
	 *         under this URI.
	 */
	protected abstract String getDatabaseSystemURI();

	/**
	 * 
	 * @return the username which allows access to the database system
	 */
	protected abstract String getDatabaseUsername();

	/**
	 * 
	 * @return the password corresponding to getDatabaseUsername()
	 */
	protected abstract String getDatabasePassword();

	/**
	 * 
	 * @return the name of the test database which shall be used
	 */
	protected abstract String getDatabaseName();

	/**
	 * this class already implements serialization/deserialization of connector
	 * state. Subclasses that have additional fields that need to be serialized
	 * to be available on slaves should return a serialized version of those
	 * fields when this method is called. If there is no such information, this
	 * method should return null.
	 * 
	 * @return serialized connector state or null if there is no such state.
	 */
	protected abstract byte[] getSerializedImplSpecificData();

	/**
	 * this method is the counterpart to getSerializedImplSpecificData() and
	 * should be able to apply any data returned from that method. It will only
	 * be called if said method returns something other than null.
	 * 
	 * @param serializedData
	 *            a serialized connector state
	 */
	protected abstract void applySerializedImplSpecificData(
			byte[] serializedData);

}
