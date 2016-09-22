package de.tuberlin.ise.benchfoundry.connectors.exceptions;

/**
 * 
 * @author joernkuhlenkamp
 *
 */
public class ConnectorException extends Exception {

	/**
	 * 
	 */
	public ConnectorException() {
		super();
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param message
	 * @param cause
	 * @param enableSuppression
	 * @param writableStackTrace
	 */
	public ConnectorException(String message, Throwable cause,
			boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param message
	 * @param cause
	 */
	public ConnectorException(String message, Throwable cause) {
		super(message, cause);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param message
	 */
	public ConnectorException(String message) {
		super(message);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param cause
	 */
	public ConnectorException(Throwable cause) {
		super(cause);
		// TODO Auto-generated constructor stub
	}

}
