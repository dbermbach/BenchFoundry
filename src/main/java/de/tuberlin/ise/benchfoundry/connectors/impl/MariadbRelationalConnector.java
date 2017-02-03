package de.tuberlin.ise.benchfoundry.connectors.impl;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.tuberlin.ise.benchfoundry.connectors.RelationalDbConnector;
import de.tuberlin.ise.benchfoundry.util.BenchFoundryConfigData;

/**
 * 
 * @author joernkuhlenkamp, dbermbach
 *
 */
public class MariadbRelationalConnector extends RelationalDbConnector {

	private static final Logger LOG = LogManager
			.getLogger(MariadbRelationalConnector.class);

	// keys in the MariaDB property file
	public final static String URI_KEY = "mariadb.uri";
	public final static String USER_KEY = "mariadb.user";
	public final static String PASS_KEY = "mariadb.password";
	public final static String DB_KEY = "mariadb.db";

	// used for caching the results from readConfigFile()
	private String URICached;
	private String UserCached;
	private String PassCached;
	private String DatabaseCached;

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * de.tuberlin.ise.benchfoundry.connectors.RelationalDbConnector#getDatabaseURI
	 * ()
	 */
	@Override
	protected String getDatabaseURI() {
		readConfigData(BenchFoundryConfigData.dbConnectorConfigFile);
		return URICached + DatabaseCached;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.tuberlin.ise.benchfoundry.connectors.RelationalDbConnector#
	 * getDatabaseSystemURI()
	 */
	@Override
	protected String getDatabaseSystemURI() {
		readConfigData(BenchFoundryConfigData.dbConnectorConfigFile);
		return URICached;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.tuberlin.ise.benchfoundry.connectors.RelationalDbConnector#
	 * getDatabaseUsername()
	 */
	@Override
	protected String getDatabaseUsername() {
		readConfigData(BenchFoundryConfigData.dbConnectorConfigFile);
		return UserCached;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.tuberlin.ise.benchfoundry.connectors.RelationalDbConnector#
	 * getDatabasePassword()
	 */
	@Override
	protected String getDatabasePassword() {
		readConfigData(BenchFoundryConfigData.dbConnectorConfigFile);
		return PassCached;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * de.tuberlin.ise.benchfoundry.connectors.RelationalDbConnector#getDatabaseName
	 * ()
	 */
	@Override
	protected String getDatabaseName() {
		readConfigData(BenchFoundryConfigData.dbConnectorConfigFile);
		return DatabaseCached;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.tuberlin.ise.benchfoundry.connectors.RelationalDbConnector#
	 * getSerializedImplSpecificData()
	 */
	@Override
	protected byte[] getSerializedImplSpecificData() {
		return null; // no implementation-specific data
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.tuberlin.ise.benchfoundry.connectors.RelationalDbConnector#
	 * applySerializedImplSpecificData(byte[])
	 */
	@Override
	protected void applySerializedImplSpecificData(byte[] serializedData) {
		// no need to do anything here unless we change
		// getSerializedImplSpecificData()
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * de.tuberlin.ise.benchfoundry.connectors.IDbConnector#readConfigData(java
	 * .lang.String)
	 */
	@Override
	public void readConfigData(String configFilename) {
		if (URICached != null)
			return; // will be executed only once
		Properties prop = new Properties();
		InputStream propIn;
		try {
			propIn = new FileInputStream(configFilename);

			prop.load(propIn);

			if (!prop.containsKey(URI_KEY)) {
				throw new IllegalArgumentException(
						"Missing property with key '" + URI_KEY
								+ "' in MariaDB configuration file '"
								+ configFilename + "'.");
			}
			if (!prop.containsKey(USER_KEY)) {
				throw new IllegalArgumentException(
						"Missing property with key '" + USER_KEY
								+ "' in MariaDB configuration file '"
								+ configFilename + "'.");
			}
			if (!prop.containsKey(PASS_KEY)) {
				throw new IllegalArgumentException(
						"Missing property with key '" + PASS_KEY
								+ "' in MariaDB configuration file '"
								+ configFilename + "'.");
			}
			if (!prop.containsKey(DB_KEY)) {
				throw new IllegalArgumentException(
						"Missing property with key '" + DB_KEY
								+ "' in MariaDB configuration file '"
								+ configFilename + "'.");
			}

			URICached = prop.getProperty(URI_KEY);
			UserCached = prop.getProperty(USER_KEY, "root");
			PassCached = prop.getProperty(PASS_KEY, "root");
			DatabaseCached = prop.getProperty(DB_KEY, "test");

			if (URICached == null || URICached.length() == 0)
				throw new IllegalArgumentException("Illegal value for key '"
						+ URI_KEY + "' in MariaDB configuration file '"
						+ configFilename + "'.");

		} catch (Exception e) {
			LOG.fatal("Cannot run the MariaDB connector of BenchFoundry"
					+ " without dbconnector config file. Reading config file "
					+ configFilename + " failed: " + e.getMessage());
			e.printStackTrace();
			System.exit(-1);

		}
	}

}
