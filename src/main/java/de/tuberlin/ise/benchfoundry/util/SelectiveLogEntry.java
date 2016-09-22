/**
 * 
 */
package de.tuberlin.ise.benchfoundry.util;

import de.tuberlin.ise.benchfoundry.scheduling.BusinessProcess;

/**
 * @author Dave
 *
 */
public class SelectiveLogEntry {

	/**
	 * if true {@link SelectiveLogEntry} will be used by {@link BusinessProcess}
	 * , this field should be checked by classes which want to use
	 * {@link SelectiveLogEntry}
	 */
	public static boolean doDetailledLogging = false;

	private StringBuilder log = new StringBuilder();

	/**
	 * adds and entry to the internal log
	 * 
	 * @param clazz
	 *            an instance of the invoking class (typically <code>this</code>
	 *            )
	 * @param message
	 *            some information to log
	 * @param e
	 *            an optional Throwable
	 */
	public void log(Object clazz, String message, Throwable e) {
		log(clazz.getClass(), message, e);
	}

	/**
	 * adds and entry to the internal log
	 * 
	 * @param clazz
	 *            an instance of the invoking class (typically this)
	 * @param message
	 *            some information to log
	 * 
	 */
	public void log(Object clazz, String message) {
		log(clazz, message, null);
	}

	/**
	 * adds and entry to the internal log
	 * 
	 * @param clazz
	 *            the invoking class
	 * @param message
	 *            some information to log
	 * @param e
	 *            an optional Throwable
	 */
	public void log(Class<?> clazz, String message, Throwable e) {
		log.append("\n\tt=" + Time.now() + " ["
				+ Thread.currentThread().getName() + ", "
				+ clazz.getSimpleName() + "]: " + message);
		if (e != null)
			log.append(" (" + e.getMessage() + ")\n" + stackTraceToString(e));
	}

	/**
	 * logs a local variable
	 * 
	 * @param name
	 *            variable name
	 * @param value
	 *            variable content
	 */
	public void logLocalVariable(String name, Object value) {
		log.append("\n\tvar " + name + "=" + value);
	}

	/**
	 * logs a local variable
	 * 
	 * @param name
	 *            variable name
	 * @param value
	 *            variable content
	 */
	public void logLocalVariable(String name, long value) {
		log.append("\n\tvar " + name + "=" + value);
	}

	/**
	 * logs a local variable
	 * 
	 * @param name
	 *            variable name
	 * @param value
	 *            variable content
	 */
	public void logLocalVariable(String name, double value) {
		log.append("\n\tvar " + name + "=" + value);
	}

	/**
	 * logs a local variable
	 * 
	 * @param name
	 *            variable name
	 * @param value
	 *            variable content
	 */
	public void logLocalVariable(String name, boolean value) {
		log.append("\n\tvar " + name + "=" + value);
	}

	/**
	 * 
	 * @param e
	 * @return a String representation of the throwable as {@link StringBuilder}
	 *         instance
	 */
	private StringBuilder stackTraceToString(Throwable e) {
		StringBuilder sb = new StringBuilder("\tstack trace:\n\t----------");
		for (StackTraceElement element : e.getStackTrace()) {
			sb.append("\n\t" + element);
		}
		sb.append("\n\t----------");
		return sb;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return log.toString();
	}

}
