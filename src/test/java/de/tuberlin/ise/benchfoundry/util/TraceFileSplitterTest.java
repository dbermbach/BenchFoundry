package de.tuberlin.ise.benchfoundry.util;

import junit.framework.TestCase;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import de.tuberlin.ise.benchfoundry.util.TraceFileSplitter;

/**
 * @author Akon Dey (akon.dey@sydney.edu.au)
 */
public class TraceFileSplitterTest extends TestCase {
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
			InputStream is = new FileInputStream("samples/trace_example.txt");
			List<OutputStream> os = new ArrayList<OutputStream>(NUM_SPLITS);
			for (int i = 0; i < NUM_SPLITS; i++) {
				os.add(new FileOutputStream("/tmp/__SprayTraceOutput__"+ i + ".txt"));
			}
			TraceFileSplitter.sprayTraceInputStreamToOutputStreams(is, os);
		} catch (FileNotFoundException e) {
			fail("Test failed with exception");
			e.printStackTrace();
		} catch (IOException e) {
			fail("Test failed with exception");
			e.printStackTrace();
		} 
	}
}
