/**
 * 
 */
package de.tuberlin.ise.benchfoundry.physicalschema.model;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * subclasses hold information on the physical schema for the respective class
 * of datastore (e.g., column store, key value store, ...). They also hold
 * information on the mapping of (logical) queries to (physical) requests [note:
 * requests are still datastore-independent and only specific to thes
 * <i>class</i> of datastore, e.g., column store requests].
 * 
 * @author Dave
 *
 */
public abstract class AbstractPhysicalSchema implements Serializable {

	/**
	 * holds for each logical query ID the corresponding list of abstract
	 * requests
	 */
	private final Map<Integer, List<? extends AbstractRequest>> requests = new HashMap<Integer, List<? extends AbstractRequest>>();

	/**
	 * retrieves a list of {@link AbstractRequest} for the given value
	 * 
	 * @param id
	 *            ID of a logical query
	 * @return desired list of requests or null if no mapping for id exists.
	 */
	public List<? extends AbstractRequest> getRequestsForQueryId(int id) {
		return requests.get(id);
	}

	/**
	 * 
	 * @return all known query/operation IDs
	 */
	public Set<Integer> getIdSet() {
		return new HashSet<Integer>(requests.keySet());
	}

	/**
	 * registers a list of requests for a given query ID
	 * 
	 * @param id
	 *            a query ID
	 * @param reqs
	 *            the corresponding list of requests
	 * @return the previous mapping for that ID
	 */
	public List<? extends AbstractRequest> registerRequestsForQueryId(
			Integer id, List<? extends AbstractRequest> reqs) {
		return requests.put(id, reqs);
	}
}
