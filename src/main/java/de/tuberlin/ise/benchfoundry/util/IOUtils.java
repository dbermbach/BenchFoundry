/**
 * 
 */
package de.tuberlin.ise.benchfoundry.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * @author Dave
 *
 */
public class IOUtils {

	private static final Logger LOG = LogManager.getLogger(IOUtils.class);

	/**
	 * 
	 * @param filename
	 * @return the content of the specified file, "# empty file", or null if an
	 *         error occured
	 */
	public static byte[] readFile(String filename) {
		if (filename == null || !new File(filename).isFile())
			return "# empty file".getBytes();
		try {
			return Files.readAllBytes(Paths.get(filename));
		} catch (IOException e) {
			LOG.error("Error while reading file " + filename + ": "
					+ e.getMessage());
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * writes content to the specified file
	 * 
	 * 
	 * @param filename
	 * @param content
	 */
	public static void writeFile(String filename, byte[] content) {
		if (content == null || filename == null)
			return;
		try {
			Files.write(Paths.get(filename), content);
		} catch (IOException e) {
			LOG.error("Error while writing file " + filename + ": "
					+ e.getMessage());
			e.printStackTrace();
		}
	}

	/**
	 * copies a file
	 * 
	 * @param in
	 *            input file
	 * @param out
	 *            output file
	 */
	public static void copyFile(String in, String out) {
		writeFile(out, readFile(in));
	}

	/**
	 * writes the byte buffer into the specified file
	 * 
	 * @param filename
	 * @param blob
	 */
	public static void writeFile(String filename, ByteBuffer blob) {
		try {
			FileOutputStream fos = new FileOutputStream(filename);
			fos.write(blob.array());
			fos.close();

		} catch (FileNotFoundException e) {
			LOG.error(filename + " is a directory!");
			e.printStackTrace();
		} catch (IOException e) {
			LOG.error("Exception while writing: " + e.getMessage(), e);
			e.printStackTrace();
		}
	}

	/**
	 * checks if the provided file exists. If not creates a file with the
	 * content "# empty file"
	 * 
	 * @param filename
	 * @return true if the file definitely exists, false otherwise
	 */
	public static boolean assertFileExists(String filename) {
		if (filename == null || filename.trim().length() == 0) {
			LOG.error("Cannot use filename that is null or only whitespaces");
			return false;
		}
		if (!new File(filename).isFile()) {
			PrintWriter pw;
			try {
				pw = new PrintWriter(filename);
				pw.println("# empty file");
				pw.close();
			} catch (Exception e) {
				LOG.error("Could not create file:" + e.getMessage());
				e.printStackTrace();
				return false;
			}
		}
		return true;
	}

}
