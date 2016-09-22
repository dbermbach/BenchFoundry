/**
 * 
 */
package de.tuberlin.ise.benchfoundry.logicalschema.queries;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.tuberlin.ise.benchfoundry.connectors.impl.DummyRelationalConnector;
import net.sf.jsqlparser.parser.ParseException;

/**
 * parses and holds all query parameters from the param and custom_param input
 * files
 * 
 * 
 * @author Dave
 *
 */
public class ParameterRegistry {

	private static final Logger LOG = LogManager.getLogger();

	/** holds the param entries for each param ID */
	private final Map<Integer, List<String>> params = new HashMap<Integer, List<String>>();

	/** holds the custom_param entries for each custom_param ID */
	private final Map<Integer, List<String>> custparams = new HashMap<Integer, List<String>>();

	/** singleton object */
	private final static ParameterRegistry instance = new ParameterRegistry();

	/** pattern for comments that shall be ignored */
	private final Pattern comment = Pattern.compile("^[ \t]*#.*$");

	/**
	 * 
	 * @return the singleton instance
	 */
	public static ParameterRegistry getInstance() {
		return ParameterRegistry.instance;
	}

	/**
	 * retrieves a param list for a given ID
	 * 
	 * @param id
	 *            id of the param list
	 * @return the list of params registered for the specified ID, throws a
	 *         {@link RuntimeException} if no mapping exists
	 */
	public List<String> getParamForID(int id) {
		List<String> res = params.get(id);
		if (res == null)
			throw new RuntimeException(
					"There was no registered parameter for the requested param ID "
							+ id);
		return res;
	}

	/**
	 * retrieves a custom_param list for a given ID
	 * 
	 * @param id
	 *            id of the custom_param list
	 * @return the list of custom_params registered for the specified ID, throws
	 *         a {@link RuntimeException} if no mapping exists
	 */
	public List<String> getCustomParamForID(int id) {
		List<String> res = custparams.get(id);
		if (res == null)
			throw new RuntimeException(
					"There was no registered custom parameter for the requested param ID "
							+ id);
		return res;
	}

	/**
	 * loads a list of parameters from the specified file. Mappings for existing
	 * IDs are replaced.
	 * 
	 * @param filename
	 *            name of the parameter file
	 */
	public void addParamFile(String filename) {
		try {
			params.putAll(parseInputStream(new FileInputStream(filename)));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	/**
	 * loads a list of custom parameters from the specified file. Mappings for
	 * existing IDs are replaced.
	 * 
	 * @param filename
	 *            name of the custom_param file
	 */
	public void addCustomParamFile(String filename) {
		if (filename == null) {
			LOG.info("Running without custom parameters.");
			return;
		}
		try {
			custparams.putAll(parseInputStream(new FileInputStream(filename)));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	/**
	 * loads a list of params from the specified input stream. Expects an
	 * id:param1;param2;...paramN format
	 * 
	 * @param is
	 *            input stream of the param file
	 * @return a map of the contents from the stream
	 */
	public Map<Integer, List<String>> parseInputStream(InputStream is) {
		Map<Integer, List<String>> result = new HashMap<>();
		String line;
		BufferedReader reader = new BufferedReader(new InputStreamReader(is));
		try {
			while ((line = reader.readLine()) != null) {
				line = line.trim();
				if (comment.matcher(line).find() || line.length() == 0)
					continue; // ignore comment lines

				String[] parts = line.split(":");
				int index = Integer.parseInt(parts[0].trim());
				String content = parts[1].trim();
				String[] pars = content.split(";");
				for (int i = 0; i < pars.length; i++)
					pars[i] = pars[i].trim();
				result.put(index, Arrays.asList(pars));
			}
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return result;
	}

	/**
	 * adds a mapping for key -1 to a list containing -1 to both param and
	 * custparam maps if they are empty
	 */
	public void assertNullParamAvailability() {
		List<String> minusOne = new ArrayList<>();
		minusOne.add("-1");
		if (!params.keySet().contains(-1))
			params.put(-1, minusOne);
		if (!custparams.keySet().contains(-1))
			custparams.put(-1, minusOne);
	}

}
