package de.tuberlin.ise.benchfoundry.util;

import junit.framework.TestCase;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import de.tuberlin.ise.benchfoundry.util.TraceFileSplitter;

/**
 * @author Akon Dey (akon.dey@sydney.edu.au)
 */
public class TraceFileSplitterTest extends TestCase {

	private static final String trace = "BOP;0;4211\n"+
			"BOT;0\n"+
			"9;4211\n"+
			"10;4212\n"+
			"11;4213\n"+
			"12;4214\n"+
			"5;4215\n"+
			"EOT\n"+
			"EOP\n"+
			"\n"+
			"BOP;0;4212\n"+
			"BOT;0\n"+
			"11;4216\n"+
			"13;4217\n"+
			"14;4218\n"+
			"14;4219\n"+
			"EOT\n"+
			"EOP";

	private static int NUM_SPLITS = 3;
	/*
	 * (non-Javadoc)
	 * 
	 * @see junit.framework.TestCase#setUp()
	 */
	protected void setUp() throws Exception {
		super.setUp();
	}
	
	/*
	 * (non-Javadoc)
	 * 
	 * @see junit.framework.TestCase#tearDown()
	 */
	protected void tearDown() throws Exception {
		super.tearDown();
	}
	
	/**
	 * Test method for
	 * {@link de.tuberlin.ise.benchfoundry.util.TraceFileSplitte#sprayTraceInputStreamToOutputStreams()}
	 * .
	 */
	public void testSprayTraceInputStreamToOutputStreams() {
		try {
			// InputStream is = new FileInputStream("samples/trace_example.txt");
			// InputStream is = new BufferedInputStream(new FileInputStream("tpcc/tpcc_run"));
			InputStream is = new StringBufferInputStream(trace);
			List<OutputStream> os = new ArrayList<OutputStream>(NUM_SPLITS);
			for (int i = 0; i < NUM_SPLITS; i++) {
				os.add(new FileOutputStream("/tmp/__SprayTraceOutput__"+ i + ".txt"));
			}
			TraceFileSplitter.sprayTraceInputStreamToOutputStreams(is, os);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			fail("Test failed with exception");
		} catch (IOException e) {
			e.printStackTrace();
			fail("Test failed with exception");
		} 
	}
}
