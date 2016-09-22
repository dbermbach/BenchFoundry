package de.tuberlin.ise.benchfoundry.results;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import de.tuberlin.ise.benchfoundry.analysis.ResultLogReader;
import de.tuberlin.ise.benchfoundry.results.BusinessOperationResult;
import de.tuberlin.ise.benchfoundry.results.RequestResult;
import de.tuberlin.ise.benchfoundry.results.ResultLogger;
import de.tuberlin.ise.benchfoundry.results.ResultType;
import de.tuberlin.ise.benchfoundry.util.BenchFoundryConfigData;
import de.tuberlin.ise.benchfoundry.util.Phase;
import de.tuberlin.ise.benchfoundry.util.PhaseManager;
import junit.framework.TestCase;

public class ResultLoggerTest extends TestCase {

	private Random r = new Random();
	private long processId = 0;
	private long transactionId = 0;
	private long operationId = 0;
	private int queryId = 0;

	private BusinessOperationResult nextResult() {
		BusinessOperationResult result = new BusinessOperationResult(processId,
				transactionId, operationId, System.currentTimeMillis() / 1000L,
				(System.currentTimeMillis() + (r.nextInt(1000 - 0))) / 1000L,
				new ArrayList<RequestResult>(), ResultType.SUCCESSFUL,
				Arrays.asList(
						Arrays.asList("Buenos Aires", "Córdoba", "La Plata"),
						Arrays.asList("red", "blue", "green"),
						Arrays.asList("elefant", "bear", "shark")), queryId);
		processId++;
		transactionId++;
		operationId++;
		return result;
	}

	public void testResultLogger() {
		fail("Not yet implemented");
	}

	public void testRun() {
		ResultLogger l = ResultLogger.getInstance();
		PhaseManager.getInstance().changePhaseTo(Phase.WARMUP);
		PhaseManager.getInstance().changePhaseTo(Phase.EXPERIMENT);
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e1) {
		}
		l.persist(nextResult());
		l.persist(nextResult());
		l.persist(nextResult());
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e1) {
		}
		PhaseManager.getInstance().changePhaseTo(Phase.TERMINATED);

	}

	public void testPersist() {
		try {
			Thread.sleep(3000);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		try {
			ResultLogReader logReader = new ResultLogReader();
			logReader.load(BenchFoundryConfigData.resultDir + "/result.log");
			logReader.dump(BenchFoundryConfigData.resultDir + "/result.csv");
		} catch (Exception e) {
			e.printStackTrace(System.err);
			fail("Unable to load ResultLogger output");
		}
		fail("Not yet implemented");
	}

	public void testSetOutputFile() {
		fail("Not yet implemented");
	}

	public void testGetOutFilePath() {
		fail("Not yet implemented");
	}

}