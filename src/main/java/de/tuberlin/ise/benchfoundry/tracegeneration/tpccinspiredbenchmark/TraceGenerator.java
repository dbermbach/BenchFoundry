package de.tuberlin.ise.benchfoundry.tracegeneration.tpccinspiredbenchmark;

import java.io.IOException;
import java.util.List;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import de.tuberlin.ise.benchfoundry.tracegeneration.tpccinspiredbenchmark.TraceFileWriter;
import de.tuberlin.ise.benchfoundry.tracegeneration.tpccinspiredbenchmark.TraceFileWriter.TraceFile;
import de.tuberlin.ise.benchfoundry.tracegeneration.tpccinspiredbenchmark.domain.Char;
import de.tuberlin.ise.benchfoundry.tracegeneration.tpccinspiredbenchmark.domain.Datetime;
import de.tuberlin.ise.benchfoundry.tracegeneration.tpccinspiredbenchmark.domain.Decimal;
import de.tuberlin.ise.benchfoundry.tracegeneration.tpccinspiredbenchmark.domain.Mediumint;
import de.tuberlin.ise.benchfoundry.tracegeneration.tpccinspiredbenchmark.domain.Smallint;
import de.tuberlin.ise.benchfoundry.tracegeneration.tpccinspiredbenchmark.domain.Tinyint;
import de.tuberlin.ise.benchfoundry.tracegeneration.tpccinspiredbenchmark.domain.Varchar;
import de.tuberlin.ise.benchfoundry.tracegeneration.tpccinspiredbenchmark.bp.ProcessBuilder;


public class TraceGenerator {

	/**
	public Table build(String name) {
		return new Table
	}
	
	public Table key() {
		
	}
	*/
	private Database db = Database.instance("tpcc")
			/*
			 * Table WAREHOUSE
			 *		W_ID		SMALLINT 		UNIQUE 	UNSIGNED
			 *		W_NAME 		VARCHAR(10)
			 *		W_STREET_1 	VARCHAR(20)
			 *		W_STREET_2 	VARCHAR(20)
			 *		W_CITY 		VARCHAR(20)
			 * 		W_STATE 	CHAR(2)
			 *		W_ZIP 		CHAR(9)
			 *		W_TAX 		DECIMAL(4,4)
			 *		W_YTD 		DECIMAL(12,2)
			 * PRIMARY KEY (W_ID)
			 */
			.addTable(Table.c("WAREHOUSE")
				.kCol(Column.c("W_ID", new Smallint(true, true, false)))
				.lCol(Column.c("W_NAME", new Varchar(false, 10)))
				.lCol(Column.c("W_STREET_1", new Varchar(false, 20)))
				.lCol(Column.c("W_STREET_2", new Varchar(false, 20)))
				.lCol(Column.c("W_CITY", new Varchar(false, 20)))
				.lCol(Column.c("W_STATE", new Char(false, 2)))
				.lCol(Column.c("W_ZIP", new Char(false, 9)))
				.lCol(Column.c("W_TAX", new Decimal(false, 4, 4)))
				.lCol(Column.c("W_YTD", new Decimal(false, 12, 2)))
			)
			/*
			 * TABLE DISTRICT
			 *		D_ID 		TINYINT 			UNIQUE	UNSIGNED
			 * 		D_W_ID 		SMALLINT 			UNIQUE	UNSIGNED
			 * 		D_NAME 		VARCHAR(10)
			 *		D_STREET_1 	VARCHAR(20)
			 * 		D_STREET_2 	VARCHAR(20)
			 *		D_CITY 		VARCHAR(20)
			 *		D_STATE 	CHAR(2)
			 *		D_ZIP 		CHAR(9)
			 *		D_TAX 		DECIMAL(4,4)
			 *		D_YTD 		DECIMAL(12,2)
			 *		D_NEXT_O_ID MEDIUMINT 			UNIQUE	UNSIGNED 
			 * PRIMARY KEY (D_W_ID, D_ID)
			 */
			.addTable(Table.c("DISTRICT")
				.kCol(Column.c("D_ID", new Tinyint(true, true, false)))
				.fCol("WAREHOUSE")
				.lCol(Column.c("D_NAME", new Varchar(false, 10)))
				.lCol(Column.c("D_STREET_1", new Varchar(false, 20)))
				.lCol(Column.c("D_STREET_2", new Varchar(false, 20)))
				.lCol(Column.c("D_CITY", new Varchar(false, 20)))
				.lCol(Column.c("D_STATE", new Char(false, 2)))
				.lCol(Column.c("D_ZIP", new Char(false, 9)))
				.lCol(Column.c("D_TAX", new Decimal(false, 4, 4)))
				.lCol(Column.c("D_YTD", new Decimal(false, 12, 2)))
				.lCol(Column.c("D_NEXT_O_ID", new Mediumint(false, true, false)))
			)
			/*
			 * TABLE CUSTOMER
			 * 		C_ID 			MEDIUMINT 			UNIQUE		UNSIGNED
			 * 		C_D_ID 			TINYINT 			UNIQUE		UNSIGNED
			 * 		C_W_ID 			SMALLINT 			UNIQUE		UNSIGNED
			 * 		C_FIRST 		VARCHAR(16)
			 * 		C_MIDDLE 		CHAR(2)
			 * 		C_LAST 			VARCHAR(16)
			 * 		C_STREET_1 		VARCHAR(20)
			 * 		C_STREET_2 		VARCHAR(20)
			 * 		C_CITY 			VARCHAR(20)
			 * 		C_STATE 		CHAR(2)
			 * 		C_ZIP 			CHAR(9)
			 * 		C_PHONE 		CHAR(16)
			 * 		C_SINCE 		DATETIME
			 * 		C_CREDIT 		CHAR(2)
			 * 		C_CREDIT_LIM 	DECIMAL(12,2)
			 * 		C_DISCOUNT 		DECIMAL(4,4)
			 * 		C_BALANCE 		DECIMAL(12,2)
			 * 		C_YTD_PAYMENT	DECIMAL(12,2)
			 * 		C_PAYMENT_CNT	DECIMAL(4)
			 * 		C_DELIVERY_CNT 	DECIMAL(4)
			 * 		C_DATA 			VARCHAR(500)
			 * PRIMARY KEY (C_W_ID, C_D_ID, C_ID)
			 */
			.addTable(Table.c("CUSTOMER")
				.kCol(Column.c("C_ID", new Mediumint(true, true, false)))
				.fCol("DISTRICT")
				.fCol("WAREHOUSE")
				.lCol(Column.c("C_FIRST", new Varchar(false, 16)))
				.lCol(Column.c("C_MIDDLE", new Char(false, 2)))
				.lCol(Column.c("C_LAST", new Varchar(false, 16)))
				.lCol(Column.c("C_STREET_1", new Varchar(false, 20)))
				.lCol(Column.c("C_STREET_2", new Varchar(false, 20)))
				.lCol(Column.c("C_CITY_1", new Varchar(false, 20)))
				.lCol(Column.c("C_STATE", new Char(false, 2)))
				.lCol(Column.c("C_ZIP", new Char(false, 9)))
				.lCol(Column.c("C_PHONE", new Char(false, 16)))
				.lCol(Column.c("C_SINCE", new Datetime(false)))
				.lCol(Column.c("C_CREDIT", new Char(false, 2)))
				.lCol(Column.c("C_CREDIT_LIM", new Decimal(false, 12, 2)))
				.lCol(Column.c("C_DISCOUNT", new Decimal(false, 4, 4)))
				.lCol(Column.c("C_BALANCE", new Decimal(false, 12, 2)))
				.lCol(Column.c("C_YTD_PAYMENT", new Decimal(false, 4)))
				.lCol(Column.c("C_DELIVERY_CNT", new Decimal(false, 4)))
				.lCol(Column.c("C_DATA", new Varchar(false, 500)))
			)
			/*
			 * TABLE ORDER
			 * 		O_ID 			MEDIUMINT 			UNIQUE		UNSIGNED
			 * 		O_D_ID			TINYINT				UNIQUE		UNSIGNED
			 * 		O_W_ID			SMALLINT			UNIQUE		UNSIGNED
			 * 		O_C_ID			MEDIUMINT			UNIQUE		UNSIGNED
			 * 		O_ENTRY_D		DATETIME
			 * 		O_CARRIER_ID	DECIMAL(1)			UNIQUE		UNSIGNED
			 * 		O_OL_CNT 		DECIMAL(2)
			 * 		O_ALL_LOCAL 	DECIMAL(1)
			 * PRIMARY KEY (O_W_ID, O_D_ID, O_ID) 
			 */
			.addTable(Table.c("ORDER")
				.kCol(Column.c("O_ID", new Mediumint(true, true, false)))
				.lCol(Column.c("O_D_ID", new Tinyint(false, true, false)))
				.lCol(Column.c("O_W_ID", new Smallint(false, true, false)))
				.lCol(Column.c("O_C_ID", new Mediumint(false, true, false)))
				.lCol(Column.c("O_ENTRY_D", new Datetime(false)))
				.lCol(Column.c("O_CARRIER_ID", new Decimal(false, 1)))
				.lCol(Column.c("O_OL_CNT", new Decimal(false, 2)))
				.lCol(Column.c("O_ALL_LOCAL", new Decimal(false, 1)))
			)
			/*
			 * TABLE NEW-ORDER
			 * 		NO_O_ID 		MEDIUMINT			UNIQUE		UNSIGNED
			 * 		NO_D_ID 		TINYINT 			UNIQUE		UNSIGNED
			 * 		NO_W_ID 		SMALLINT 			UNIQUE		UNSIGNED
			 * PRIMARY KEY (NO_W_ID, NO_D_ID, NO_O_ID)
			 */
			.addTable(Table.c("NEW-ORDER")
				.kCol(Column.c("NO_ID", new Mediumint(true, true, false)))
				.fCol("DISTRICT")
				.fCol("WAREHOUSE")
				.lCol(Column.c("NO_DATA", new Char(false, 8)))
			)
			/*
			 * TABLE ITEM
			 * 		I_ID 			MEDIUMINT 			UNIQUE		UNSIGNED
			 * 		I_IM_ID 		MEDIUMINT 			UNIQUE		UNSIGNED
			 * 		I_NAME			VARCHAR(24)
			 * 		I_PRICE 		DECIMAL(5,2)
			 * 		I_DATA 			VARCHAR(50)
			 * PRIMARY KEY (I_ID) )
			 */
			.addTable(Table.c("ITEM")
				.kCol(Column.c("I_ID", new Mediumint(true, true, false)))
				.lCol(Column.c("I_NAME", new Varchar(false, 24)))
				.lCol(Column.c("I_PRICE", new Decimal(false, 5, 2)))
				.lCol(Column.c("I_DATA", new Varchar(false, 50)))
			)
			/*
			 * TABLE STOCK
			 * 		S_I_ID 			MEDIUMINT 			UNIQUE		UNSIGNED
			 * 		S_W_ID 			SMALLINT 			UNIQUE		UNSIGNED
			 * 		S_QUANTITY 		DECIMAL(4)
			 * 		S_DIST_01 		CHAR(24)
			 * 		S_DIST_02 		CHAR(24)
			 * 		S_DIST_03 		CHAR(24)
			 * 		S_DIST_04 		CHAR(24)
			 * 		S_DIST_05 		CHAR(24)
			 * 		S_DIST_06 		CHAR(24)
			 * 		S_DIST_07 		CHAR(24)
			 * 		S_DIST_08 		CHAR(24)
			 * 		S_DIST_09 		CHAR(24)
			 * 		S_DIST_10 		CHAR(24)
			 * 		S_YTD 			DECIMAL(8)
			 * 		S_ORDER_CNT 	DECIMAL(4)
			 * 		S_REMOTE_CNT 	DECIMAL(4)
			 * 		S_DATA 			VARCHAR(50)
			 * PRIMARY KEY (S_W_ID, S_I_ID)
			 */
			.addTable(Table.c("STOCK")
				.kCol(Column.c("S_ID", new Mediumint(true, true, false)))
				.fCol("ITEM")
				.fCol("WAREHOUSE")
				.lCol(Column.c("S_QUANTITY", new Decimal(false, 4)))
				.lCol(Column.c("S_DIST_01", new Char(false, 24)))
				.lCol(Column.c("S_DIST_02", new Char(false, 24)))
				.lCol(Column.c("S_DIST_03", new Char(false, 24)))
				.lCol(Column.c("S_DIST_04", new Char(false, 24)))
				.lCol(Column.c("S_DIST_05", new Char(false, 24)))
				.lCol(Column.c("S_DIST_06", new Char(false, 24)))
				.lCol(Column.c("S_DIST_07", new Char(false, 24)))
				.lCol(Column.c("S_DIST_08", new Char(false, 24)))
				.lCol(Column.c("S_DIST_09", new Char(false, 24)))
				.lCol(Column.c("S_DIST_10", new Char(false, 24)))
				.lCol(Column.c("S_YTD", new Decimal(false, 8)))
				.lCol(Column.c("S_ORDER_CNT", new Decimal(false, 4)))
				.lCol(Column.c("S_REMOTE_CNT", new Decimal(false, 4)))
				.lCol(Column.c("S_DATA", new Varchar(false, 50)))
			)
			/*
			 * TABLE ORDER-LINE
			 * 		OL_O_ID 		MEDIUMINT			UNIQUE		UNSIGNED
			 * 		OL_D_ID 		TINYINT				UNIQUE		UNSIGNED
			 * 		OL_W_ID 		SMALLINT			UNIQUE		UNSIGNED
			 * 		OL_NUMBER 		DECIMAL(2)			UNIQUE		UNSIGNED
			 * 		OL_I_ID 		MEDIUMINT			UNIQUE		UNSIGNED
			 * 		OL_SUPPLY_W_ID	SMALLINT
			 * 		OL_DELIVERY_D	DATETIME
			 * 		OL_QUANTITY		DECIMAL(2)
			 * 		OL_AMOUNT		DECIMAL(6,2)
			 * 		OL_DIST_INFO	CHAR(24)
			 * PRIMARY KEY (OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER)
			 */
			.addTable(Table.c("ORDER-LINE")
				.kCol(Column.c("OL_NUMBER", new Mediumint(true, true, false)))
				.fCol("ORDER")
				.fCol("DISTRICT")
				.fCol("WAREHOUSE")
				.fCol("ITEM")
				.lCol(Column.c("OL_SUPPLY_W_ID", new Smallint(false, true, false)))
				.lCol(Column.c("OL_DELIVERY_D", new Datetime(false)))
				.lCol(Column.c("OL_QUANTITY", new Decimal(false, 2)))
				.lCol(Column.c("OL_AMOUNT", new Decimal(false, 6, 2)))
				.lCol(Column.c("OL_DIST_INFO", new Char(false, 24)))
			)
			/*
			 * TABLE HISTORY
			 * 		H_C_ID 			MEDIUMINT 			UNIQUE		UNSIGNED
			 * 		H_C_D_ID 		TINYINT 			UNIQUE		UNSIGNED
			 * 		H_C_W_ID 		SMALLINT 			UNIQUE		UNSIGNED
			 * 		H_D_ID 			TINYINT 			UNIQUE		UNSIGNED
			 * 		H_W_ID 			SMALLINT 			UNIQUE		UNSIGNED
			 * 		H_DATE 			DATETIME
			 * 		H_AMOUNT 		DECIMAL(6,2)
			 * 		H_DATA 			VARCHAR(24)
			 */
			.addTable(Table.c("HISTORY")
				.kCol(Column.c("H_ID", new Mediumint(true, true, false)))
				.lCol(Column.c("H_DATE", new Datetime(false)))
				.lCol(Column.c("H_AMOUNT", new Decimal(false, 6, 2)))
				.lCol(Column.c("H_DATA", new Varchar(false, 24)))
			);
				
	
	// LOAD TRACE PARAMTERS
	// Scales the initial dataset for the benchmark
	@Parameter(names={"--datasetScaler"})
	private int datasetScaler = 1;
	
	// RUN TRACE PARAMTERS
	// Overall runtime of the benchmark in seconds
	@Parameter(names={"--runtime"})
	private int runtime = 120;
	
	// Scales the base number of all processes per seconds
	@Parameter(names={"--processTargetScaler"})
	private int processTargetScaler = 10;
	
	// Base number of process "payment" per second
	@Parameter(names={"--paymentProcessTarget"})
	private int paymentProcessTarget = 1;
	
	// Base number of process "orderstatus" per second
	@Parameter(names={"--orderstatusProcessTarget"})
	private int orderstatusProcessTarget = 1;
	
	// Base number of process "neworder" per second
	@Parameter(names={"--neworderProcessTarget"})
	private int neworderProcessTarget = 1;
	
	public Database getDB(){
		return db;
	}
	
	public void run() {
		/**
		// GENERATE LOAD TRACE
		// Scales the initial dataset for the benchmark
		int datasetScaler = 1;
		
		// GENERATE RUN TRACE
		// Overall runtime of the benchmark in seconds
		int runtime = 120;
		// Scales the base number of all processes per seconds
		int processTargetScaler = 250;
		// Base number of process "payment" per second 
		int paymentProcessTarget = 1;
		// Base number of process "orderstatus" per second
		int orderstatusProcessTarget = 1;
		// Base number of process "neworder" per second
		int neworderProcessTarget = 1;
		*/
		
		Database db = new TraceGenerator().getDB();
		System.out.println("Database: " +db.getName().toUpperCase());
		List<Table> tables = db.getTables();
		/**
		for (Table table : tables) {
			System.out.println("TABLE " +table.getName().toUpperCase());
			System.out.println("   "+table.getInsertOp());
			for (int i = 0; i < 3; i++) {
				System.out.println("   " +String.join(", ", table.getInsertParams()));
			}
			System.out.println("   "+table.getReadOp());
			for (int i = 0; i < 3; i++) {
				System.out.println("   " +String.join(", ", table.getReadParams()));
			}
			System.out.println("   "+table.getUpdateOp());
			for (int i = 0; i < 3; i++) {
				System.out.println("   " +String.join(", ", table.getUpdateParams()));
			}
		}
		*/
		
		String benchmarkName = "tpcc";
		TraceFileWriter writer = new TraceFileWriter(benchmarkName);
		try {
			System.out.println("Creating directory structure and touching files...");
			writer.touchFiles();
		} catch (IOException e) {
			System.out.println("Not able to create output files. Exiting program now. Stacktrace: ");
			e.printStackTrace();
			System.exit(1);
		}
		
		try {
			System.out.println("Creating content of main properties file...");
			writer.append(TraceFile.PROPS, "# location of the schema input file which should be a sequence of DDL statements");
			writer.append(TraceFile.PROPS, "schemaFilename:" +benchmarkName+ "_schema");
			
			writer.append(TraceFile.PROPS, "# location of the file containing all the queries used in the respective trace");
			writer.append(TraceFile.PROPS, "oplistFilename:" +benchmarkName+ "_operation");
			
			writer.append(TraceFile.PROPS, "# location of the file containing all parameters sets refered to in the trace files");
			writer.append(TraceFile.PROPS, "paramFilename:" +benchmarkName+ "_param");
			
			writer.append(TraceFile.PROPS, "# location of the file containing all parameters sets refered to in the trace files");
			writer.append(TraceFile.PROPS, "custparamFilename:" +benchmarkName+ "_cparam");
			
			writer.append(TraceFile.PROPS, "# location of the trace file which will preload the database with an initial data set this trace may contain to entries");
			writer.append(TraceFile.PROPS, "preloadTraceFilename:" +benchmarkName+ "_load");
			
			writer.append(TraceFile.PROPS, "# location of the trace file which will be used to warm up the SUT");
			writer.append(TraceFile.PROPS, "warmupTraceFilename:" +benchmarkName+ "_warm");
			
			writer.append(TraceFile.PROPS, "# location of the trace file of the actual experiment");
			writer.append(TraceFile.PROPS, "experimentTraceFilename:" +benchmarkName+ "_run");
			
			writer.append(TraceFile.PROPS, "# directory where the output results shall be written to on all machines");
			writer.append(TraceFile.PROPS, "resultDir:results");
			
			writer.append(TraceFile.PROPS, "# factory class which will create a physical schema from the logical schema specified in the schema input file.");
			writer.append(TraceFile.PROPS, "physicalSchemFactoryClass:de.tuberlin.ise.benchmw.physicalschema.factory.RelationalPhysicalSchemaFactory");
			
			writer.append(TraceFile.PROPS, "# class which shall be used to connect to the SUT.");
			writer.append(TraceFile.PROPS, "dbConnectorClass:de.tuberlin.ise.benchmw.connectors.impl.MariadbRelationalConnector");
			
			writer.append(TraceFile.PROPS, "# BenchMW will issue a prepare command to the database connector before sending the execute command at the poin in time specified in the trace.");
			writer.append(TraceFile.PROPS, "transactionPrepareTimeInMs:1000");
			
			writer.append(TraceFile.PROPS, "# BenchMW will submit process instances to a thread pool for execution prior to the specified timestamp from the trace.");
			writer.append(TraceFile.PROPS, "processScheduleAheadTimeInMs:500");
			
			writer.append(TraceFile.PROPS, "# The file that contains information on all slave instances. Use slaveFile:none if the master shall be run as single node BenchMW instance");
			writer.append(TraceFile.PROPS, "slaveFile:none");
			
			writer.append(TraceFile.PROPS, "# If this flag is set to true, then BenchMW will print a detailed error trace for each business process with an exception.");
			writer.append(TraceFile.PROPS, "doDetailledLoggingForExceptions:false");
		} catch(IOException e) {
			System.out.println("Not able to create main properties file. Exiting program now. Stacktrace: ");
			e.printStackTrace();
			System.exit(1);
		}
		
		System.out.println("Creating content for schema file...");
		for (Table table : tables) {
			try {
				writer.append(TraceFile.SCHEMA, table.getDdl());
			} catch (IOException e) {
				System.out.println("Not able to add DDL statements for table " +table.getName()+ ". Exiting now!");
				e.printStackTrace();
				System.exit(1);
			}
			System.out.println("Successfully added DDL statements for table " +table.getName()+ ".");
		}
		
		ProcessBuilder b = new ProcessBuilder(writer);
		
		
		try {
			System.out.println("Creating content of load trace file...");
			for(int i = 0; i < 1*datasetScaler; i++) {
				b.p(0).t(0).o(
					Table.getInstance("WAREHOUSE").getInsertOp(), 
					Table.getInstance("WAREHOUSE").insertParams())
					.build(TraceFile.LOAD);
			}

			for(int i = 0; i < 10*datasetScaler; i++) {
				b.p(0).t(0).o(
					Table.getInstance("DISTRICT").getInsertOp(), 
					Table.getInstance("DISTRICT").insertParams())
					.build(TraceFile.LOAD);
			}
			for(int i = 0; i < 1000*datasetScaler; i++) {
				b.p(0).t(0).o(
					Table.getInstance("ITEM").getInsertOp(), 
					Table.getInstance("ITEM").insertParams())
					.build(TraceFile.LOAD);
			}
			for(int i = 0; i < 300*datasetScaler; i++) {
				b.p(0).t(0).o(
					Table.getInstance("CUSTOMER").getInsertOp(), 
					Table.getInstance("CUSTOMER").insertParams())
					.build(TraceFile.LOAD);
			}
			for(int i = 0; i < 300*datasetScaler; i++) {
				b.p(0).t(0).o(
					Table.getInstance("ORDER").getInsertOp(), 
					Table.getInstance("ORDER").insertParams())
					.build(TraceFile.LOAD);
			}
			for(int i = 0; i < 300*datasetScaler; i++) {
				b.p(0).t(0).o(
					Table.getInstance("HISTORY").getInsertOp(), 
					Table.getInstance("HISTORY").insertParams())
					.build(TraceFile.LOAD);
			}
			for(int i = 0; i < 300*datasetScaler; i++) {
				b.p(0).t(0).o(
					Table.getInstance("NEW-ORDER").getInsertOp(), 
					Table.getInstance("NEW-ORDER").insertParams())
					.build(TraceFile.LOAD);
			}
			for(int i = 0; i < 1000*datasetScaler; i++) {
				b.p(0).t(0).o(
					Table.getInstance("STOCK").getInsertOp(), 
					Table.getInstance("STOCK").insertParams())
					.build(TraceFile.LOAD);
			}
			for(int i = 0; i < 1000*datasetScaler; i++) {
				b.p(0).t(0).o(
					Table.getInstance("ORDER-LINE").getInsertOp(), 
					Table.getInstance("ORDER-LINE").insertParams())
					.build(TraceFile.LOAD);
			}
		} catch (IOException e) {
			System.out.println("Not able to add Load DML statements. Exiting now!");
			e.printStackTrace();
			System.exit(1);
		}
		
		try {
			System.out.println("Creating content of run trace file...");
			for(int interval = 0; interval < runtime; interval++) {
				for(int i = 0; i < paymentProcessTarget*processTargetScaler; i++) {
					addPaymentTransaction(interval*1000, b);
				}
				for(int i = 0; i < orderstatusProcessTarget*processTargetScaler; i++) {
					addOrderstatusTransaction(interval*1000, b);
				}
			}
			
		} catch (IOException e) {
			System.out.println("Not able to add Run DML statements. Exiting now!");
			e.printStackTrace();
			System.exit(1);
		}	
		
	}
	
	private static ProcessBuilder addNeworderTransaction(long start, ProcessBuilder b) throws IOException {
		b.p(start).t(0)
			.o(Table.getInstance("WAREHOUSE").getReadOp(), Table.getInstance("WAREHOUSE").readParams())
			.o(Table.getInstance("DISTRICT").getReadOp(), Table.getInstance("DISTRICT").readParams())
			.o(Table.getInstance("CUSTOMER").getReadOp(), Table.getInstance("CUSTOMER").readParams())
			.o(Table.getInstance("NEW-ORDER").getInsertOp(), Table.getInstance("NEW-ORDER").insertParams())
			.o(Table.getInstance("ORDER").getInsertOp(), Table.getInstance("ORDER").insertParams())
			.o(Table.getInstance("ORDER-LINE").getInsertOp(), Table.getInstance("ORDER-LINE").insertParams())
			.o(Table.getInstance("ITEM").getReadOp(), Table.getInstance("ITEM").readParams())
			.o(Table.getInstance("ORDER-LINE").getInsertOp(), Table.getInstance("ORDER-LINE").insertParams())
			.o(Table.getInstance("ITEM").getReadOp(), Table.getInstance("ITEM").readParams())
			.o(Table.getInstance("ORDER-LINE").getInsertOp(), Table.getInstance("ORDER-LINE").insertParams())
			.o(Table.getInstance("ITEM").getReadOp(), Table.getInstance("ITEM").readParams())
			.build(TraceFile.RUN);
		return b;
	}
	
	/**
	 * Adds a Payment Transaction to the trace
	 * READ WAREHOUSE
	 * READ DISTRICT
	 * READ CUSTOMER
	 * UPDATE CUSTOMER
	 * INSERT HISTORY
	 * @param b
	 * @return
	 * @throws IOException 
	 */
	private static ProcessBuilder addPaymentTransaction(long start, ProcessBuilder b) throws IOException {
		b.p(start).t(0)
			.o(Table.getInstance("WAREHOUSE").getReadOp(), Table.getInstance("WAREHOUSE").readParams())
			.o(Table.getInstance("DISTRICT").getReadOp(), Table.getInstance("DISTRICT").readParams())
			.o(Table.getInstance("CUSTOMER").getReadOp(), Table.getInstance("CUSTOMER").readParams())
			.o(Table.getInstance("CUSTOMER").getUpdateOp(), Table.getInstance("CUSTOMER").updateParams())
			.o(Table.getInstance("HISTORY").getInsertOp(), Table.getInstance("HISTORY").insertParams())
			.build(TraceFile.RUN);
		return b;
	}
	
	private static ProcessBuilder addOrderstatusTransaction(long start, ProcessBuilder b) throws IOException {
		b.p(start).t(0)
			.o(Table.getInstance("CUSTOMER").getReadOp(), Table.getInstance("CUSTOMER").readParams())
			.o(Table.getInstance("ORDER").getReadOp(), Table.getInstance("ORDER").readParams())
			.o(Table.getInstance("ORDER-LINE").getReadOp(), Table.getInstance("ORDER-LINE").readParams())
			.o(Table.getInstance("ORDER-LINE").getReadOp(), Table.getInstance("ORDER-LINE").readParams())
			.build(TraceFile.RUN);
		return b;
	}
	
	public static void main(String[] args) {
		TraceGenerator main = new TraceGenerator();
        new JCommander(main, args);
        main.run();
	}
}
