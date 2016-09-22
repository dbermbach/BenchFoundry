/**
 * 
 */
package de.tuberlin.ise.benchfoundry.logicalschema.queries;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import de.tuberlin.ise.benchfoundry.util.QueryInputFileParser;
import net.sf.jsqlparser.parser.ParseException;

/**
 * holds all {@link LogicalQuery} instances and allows look-ups of
 * {@link LogicalQuery} objects via their id.
 * 
 * 
 * @author Dave
 *
 */
public class LogicalQueryRegistry {

	/** singleton object */
	private final static LogicalQueryRegistry instance = new LogicalQueryRegistry();

	/** holds all instances of LogicalQuery, key is the query id */
	private final Map<Integer, LogicalQuery> queries = new HashMap<Integer, LogicalQuery>();

	private final Pattern comment = Pattern.compile("^[ \t]*#.*$");

	/**
	 * 
	 * @return the singleton instance of {@link LogicalQueryRegistry}
	 */
	public static LogicalQueryRegistry getInstance() {
		return LogicalQueryRegistry.instance;
	}

	/**
	 * 
	 * retrieves a query by its ID
	 * 
	 * @param id
	 *            the ID of the {@link LogicalQuery} desired
	 * @return the respective {@link LogicalQuery} instance or null if no such
	 *         query exists.
	 */
	public LogicalQuery getQueryForId(int id) {
		return queries.get(id);
	}

	/**
	 * retrieves all queries of a specific type
	 * 
	 * @param returnSet
	 *            new set that is returned as result
	 * @param clazz
	 *            the subclass of {@link LogicalQuery} of which instances shall
	 *            be returned
	 * @return returnSet. Adds all instances of clazz that are known to this
	 *         registry to returnSet.
	 */
	@SuppressWarnings("unchecked")
	public <T extends LogicalQuery> Set<T> getQueriesByType(Set<T> returnSet,
			Class<T> clazz) {
		for (LogicalQuery lq : queries.values()) {
			if (clazz.isInstance(lq))
				returnSet.add((T) lq);
		}
		return returnSet;
	}

	/**
	 * loads a list of queries from the specified file. Mappings for existing
	 * IDs are replaced.
	 * 
	 * @param filename
	 *            name of the query file
	 */
	public void addQueryInputFile(String filename) throws IOException,
			ParseException {
		addQueryInputFile(new FileInputStream(new File(filename)));
	}

	/**
	 * loads a list of queries from the specified input stream. Mappings for
	 * existing IDs are replaced.
	 * 
	 * @param is
	 *            input stream of the query file
	 */
	public void addQueryInputFile(InputStream is) throws IOException,
			ParseException {
		for (Entry<Integer, String> entry : QueryInputFileParser
				.readQueryInputFileFromStream(is).entrySet()) {
			registerQuery(LogicalQuery.makeQuery(entry.getValue(),
					entry.getKey()));
		}
	}

	/**
	 * registers a new {@link LogicalQuery}. Replaces existing queries with the
	 * same ID
	 * 
	 * @param query
	 */
	public void registerQuery(LogicalQuery query) {
		queries.put(query.getId(), query);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer("Registered queries:");
		for (Entry<Integer, LogicalQuery> e : queries.entrySet())
			sb.append("\n\t" + e.getKey() + ": " + e.getValue());
		return sb.toString();
	}

	/**
	 * @return
	 * @see java.util.Map#size()
	 */
	public int getNumberOfQueries() {
		return this.queries.size();
	}

}
