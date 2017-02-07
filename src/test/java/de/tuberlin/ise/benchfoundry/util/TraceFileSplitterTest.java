package de.tuberlin.ise.benchfoundry.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringBufferInputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import junit.framework.TestCase;

/**
 * @author Akon Dey (akon.dey@sydney.edu.au)
 */
public class TraceFileSplitterTest extends TestCase {

	private static final String trace = "BOP;0;4211\n" + "BOT;0\n" + "9;4211\n"
			+ "10;4212\n" + "11;4213\n" + "12;4214\n" + "5;4215\n" + "EOT\n"
			+ "EOP\n" + "\n" + "BOP;0;4212\n" + "BOT;0\n" + "11;4216\n"
			+ "13;4217\n" + "14;4218\n" + "14;4219\n" + "EOT\n" + "EOP";

	private static final String[] splits = {
			"BOP;0;4211\n" + "BOT;0\n" + "9;4211;-1\n" + "10;4212;-1\n"
					+ "11;4213;-1\n" + "12;4214;-1\n" + "5;4215;-1\n" + "EOT\n"
					+ "EOP\n",
			"BOP;0;4212\n" + "BOT;0\n" + "11;4216;-1\n" + "13;4217;-1\n"
					+ "14;4218;-1\n" + "14;4219;-1\n" + "EOT\n" + "EOP" };

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

			InputStream is = new StringBufferInputStream(trace);
			List<OutputStream> os = new ArrayList<OutputStream>(NUM_SPLITS);
			for (int i = 0; i < NUM_SPLITS; i++) {
				os.add(new ByteArrayOutputStream());
			}

			TraceFileSplitter.sprayTraceInputStreamToOutputStreams(is, os);
			Set<String> createdSplits = new HashSet<>(), original = new HashSet<>();
			original.add(splits[0].trim());
			original.add(splits[1].trim());
			boolean alreadyContained;
			for (OutputStream o : os) {
				String part = new String(
						((ByteArrayOutputStream) o).toByteArray()).trim();
				if (part == null || part.length() == 0)
					continue;
				alreadyContained = !createdSplits.add(part);
				if (alreadyContained)
					fail("Test failed since the output trace parts contained a duplicate entry.");
			}
			if (!(createdSplits.containsAll(original) && original
					.containsAll(createdSplits))) {
//				System.out.println("original:" + original);
//				System.out.println("splits:" + createdSplits);
				fail("The union of the trace splits was not identical to the original trace.");
			}

		} catch (IOException e) {
			e.printStackTrace();
			fail("Test failed with exception");
		}
	}
}
