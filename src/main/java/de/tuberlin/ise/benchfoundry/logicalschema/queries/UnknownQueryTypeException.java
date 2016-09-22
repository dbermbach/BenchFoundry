/**
 * @author Akon Dey (akon.dey@gmail.com)
 *
 */
package de.tuberlin.ise.benchfoundry.logicalschema.queries;

import net.sf.jsqlparser.parser.ParseException;

/**
 * @author Akon Dey
 *
 */
public class UnknownQueryTypeException extends ParseException {
	private static final long serialVersionUID = -222726977986202144L;

	/**
	 * 
	 */
	public UnknownQueryTypeException() {
		super();
	}

	/**
	 * @param message
	 */
	public UnknownQueryTypeException(String message) {
		super(message);
	}
}
