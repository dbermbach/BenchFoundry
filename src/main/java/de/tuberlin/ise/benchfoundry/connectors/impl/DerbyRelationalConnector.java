/**
 * 
 */
package de.tuberlin.ise.benchfoundry.connectors.impl;

import de.tuberlin.ise.benchfoundry.connectors.RelationalDbConnector;

/**
 * @author Dave
 *
 */
public class DerbyRelationalConnector extends RelationalDbConnector {

//	private static final Logger LOG = LogManager
//			.getLogger(DerbyRelationalConnector.class);

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * de.tuberlin.ise.benchfoundry.connectors.RelationalDbConnector#getDatabaseURI
	 * ()
	 */
	@Override
	protected String getDatabaseURI() {
		return "jdbc:derby:memory:derby;create=true";
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.ise.benchfoundry.connectors.RelationalDbConnector#getDatabaseSystemURI()
	 */
	@Override
	protected String getDatabaseSystemURI() {
		return null;
	}
	
	/*
	 * (non-Javadoc)
	 * 
	 * @see de.tuberlin.ise.benchfoundry.connectors.RelationalDbConnector#
	 * getDatabaseUsername()
	 */
	@Override
	protected String getDatabaseUsername() {
		return "sa";
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.tuberlin.ise.benchfoundry.connectors.RelationalDbConnector#
	 * getDatabasePassword()
	 */
	@Override
	protected String getDatabasePassword() {
		return "";
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
		return "derby";
	}

	

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.tuberlin.ise.benchfoundry.connectors.RelationalDbConnector#
	 * getSerializedImplSpecificData()
	 */
	@Override
	protected byte[] getSerializedImplSpecificData() {
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.tuberlin.ise.benchfoundry.connectors.RelationalDbConnector#
	 * applySerializedImplSpecificData(byte[])
	 */
	@Override
	protected void applySerializedImplSpecificData(byte[] serializedData) {
		// no need to do anything here unless getSerializedImplSpecificData() no
		// longer returns null

	}

	/* (non-Javadoc)
	 * @see de.tuberlin.ise.benchfoundry.connectors.IDbConnector#readConfigData(java.lang.String)
	 */
	@Override
	public void readConfigData(String configFilename) {
		//no need to do anything here
	}

//	public static void main(String[] args) throws Exception {
//		LoggerContext context = (org.apache.logging.log4j.core.LoggerContext) LogManager
//				.getContext(false);
//		File file = new File("log4j2.xml");
//		context.setConfigLocation(file.toURI());
//
//		DerbyRelationalConnector derby = new DerbyRelationalConnector();
//		derby.init();
//		Connection dbConnection = derby.getDbConnection();
//
//		Statement stmt = dbConnection.createStatement();
//		// drop table
//		// stmt.executeUpdate("Drop Table users");
//
//		// create table
//		stmt.executeUpdate("Create table users (id int primary key, name varchar(30))");
//
//		// insert 2 rows
//		stmt.executeUpdate("insert into users values (1,'tom')");
//		stmt.executeUpdate("insert into users values (2,'peter')");
//
//		// query
//		ResultSet rs = stmt.executeQuery("SELECT * FROM users");
//
//		// print out query result
//		while (rs.next()) {
//			System.out
//					.printf("%d\t%s\n", rs.getInt("id"), rs.getString("name"));
//		}
//
//		derby.cleanUpDatabase();
//	}

	

}
