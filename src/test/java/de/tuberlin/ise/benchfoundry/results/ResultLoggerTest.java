package de.tuberlin.ise.benchfoundry.results;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.stream.Stream;

import de.tuberlin.ise.benchfoundry.analysis.ResultLogReader;
import de.tuberlin.ise.benchfoundry.util.BenchFoundryConfigData;
import de.tuberlin.ise.benchfoundry.util.Phase;
import de.tuberlin.ise.benchfoundry.util.PhaseManager;
import junit.framework.TestCase;
import org.junit.Before;
import org.junit.Test;

public class ResultLoggerTest extends TestCase {

    private Random r = new Random();
    private long processId = 0;
    private long transactionId = 0;
    private long operationId = 0;
    private int queryId = 0;

    private BusinessOperationResult nextMeasurement() {
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

    @Before
    public void resetMeasurementFactory() {
        processId = 0;
        transactionId = 0;
        operationId = 0;
    }

    @Test
    public void testRun() {
        ResultLogger l = ResultLogger.getInstance();
        PhaseManager.getInstance().changePhaseTo(Phase.WARMUP);
        PhaseManager.getInstance().changePhaseTo(Phase.EXPERIMENT);
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e1) {
        }
        l.persist(nextMeasurement());
        l.persist(nextMeasurement());
        l.persist(nextMeasurement());
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e1) {
        }
        PhaseManager.getInstance().changePhaseTo(Phase.TERMINATED);

    }

    @Test
    public void testDump() {
        final String dumpFileDir = BenchFoundryConfigData.resultDir;
        final String dumpFileName = BenchFoundryConfigData.name + "-result.log";
        try {
            ResultLogReader logReader = new ResultLogReader();
            logReader.update(nextMeasurement());
            logReader.dump(dumpFileName);
        } catch (Exception e) {
            e.printStackTrace(System.err);
            fail("Unable to dump measurements via ResultLogReader.");
        }
        try (Stream<String> stream = Files.lines(Paths.get(dumpFileDir, dumpFileName))) {
            String[] lines = (String[]) stream.toArray();
            String[] line = lines[0].split(";");
            assertEquals("ProcessId is 0", line[0], 0);
            assertEquals("TransactionId is 0", line[1], 0);
            assertEquals("OperationId is 0", line[2], 0);
            assertEquals("ResultType is SUCCESS", line[7], "SUCCESSFUL");
        } catch (IOException e) {
            e.printStackTrace();
            fail("Unable to stream measurements from dump file.");
        }
    }
}