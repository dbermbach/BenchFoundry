/**
 * 
 */
package de.tuberlin.ise.benchfoundry.physicalschema.model;

/**
 * describes a request - concrete subclasses should specify behavior and
 * content. This class just serves as a wrapper/common super class.
 * 
 * @author Dave
 *
 */
public abstract class AbstractRequest {

	/** Id of the surrounding operation. */
	private final int logicalQueryId;

	public AbstractRequest(int logicalQueryId) {
		super();
		this.logicalQueryId = logicalQueryId;
	}

	public int getLogicalQueryId() {
		return logicalQueryId;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "AbstractRequest [logicalQueryId=" + this.logicalQueryId + "]";
	}

	// FIXME maybe add some generic stuff like IDs etc.?

	
	
}
