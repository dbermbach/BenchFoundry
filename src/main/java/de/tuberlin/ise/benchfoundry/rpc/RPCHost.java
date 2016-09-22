/**
 * 
 */
package de.tuberlin.ise.benchfoundry.rpc;

/**
 * @author Dave
 *
 */
public class RPCHost {
	
	public final String host;
	
	public final int port;
	
	public final String name;

	/**
	 * @param host
	 * @param port
	 * @param name
	 */
	public RPCHost(String host, int port, String name) {
		super();
		this.host = host;
		this.port = port;
		this.name = name;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "RPCHost ["+this.name + "@"+ this.host+":"+this.port + "]";
	}

	
	
}
