/**
 * 
 */
package de.tuberlin.ise.benchfoundry.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * @author Dave
 *
 */
public class QueryInputFileParser {

	private final static Pattern comment = Pattern.compile("^[ \t]*#.*$");

	/**
	 * reads a query input file in the form id:query from the specified stream.
	 * Lines starting with # as the first non-whitespace character are ignored
	 * as comment lines.
	 * 
	 * 
	 * @param is
	 *            input stream to be read
	 * @return a map containing everything that could be read from the stream
	 * @throws IOException
	 */
	public static Map<Integer, String> readQueryInputFileFromStream(
			InputStream is) throws IOException {
		Map<Integer, String> result = new HashMap<>();
		String line;
		BufferedReader reader = new BufferedReader(new InputStreamReader(is));
		while ((line = reader.readLine()) != null) {
			if (comment.matcher(line).find()||line.trim().length()==0)
				continue; // ignore comment lines
			String[] parts = line.split(":");
			result.put(Integer.parseInt(parts[0]), parts[1]);
		}
		return result;
	}

}
