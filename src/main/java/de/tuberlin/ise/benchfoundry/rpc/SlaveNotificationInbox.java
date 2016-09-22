/**
 * 
 */
package de.tuberlin.ise.benchfoundry.rpc;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * here are flags to be set so that the Starter class for slave instances can do
 * blocking calls to wait for particular states changes
 * 
 * 
 * 
 * @author Dave
 *
 */
public class SlaveNotificationInbox {

	private static final Map<InboxTopic, LockAndFieldTuple> locks = new HashMap<>();

	static {
		for (InboxTopic topic : InboxTopic.values()) {
			locks.put(topic, new LockAndFieldTuple());
		}
	}

	/**
	 * wakes up the blocked threads and sets the field.
	 */
	private static void wakeUpWaitingThreads(InboxTopic lockField) {
		LockAndFieldTuple laft = locks.get(lockField);
		laft.lock.lock();
		try {
			laft.value = true;
			laft.condition.signalAll();
		} finally {
			laft.lock.unlock();
		}
	}

	/**
	 * blocks until someone sets the respective inbox field. Will reset
	 * interrupted flag.
	 */
	private static void waitForSignalOnItem(InboxTopic lockField) {
		LockAndFieldTuple laft = locks.get(lockField);
		laft.lock.lock();
		try {
			while (!laft.value) {
				try {
					laft.condition.await();
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					continue;
				}
			}
		} finally {
			laft.lock.unlock();
		}
	}

	/**
	 * blocks until the signal to proceed to preload phase has been received
	 */
	public static void waitForStartPreload() {
		waitForSignalOnItem(InboxTopic.PROCEED_TO_PRELOAD);
	}

	/**
	 * blocks until the configuration details have been received
	 */
	public static void waitForConfigurationDetails() {
		waitForSignalOnItem(InboxTopic.CONFIG_DETAILS_RECEIVED);
	}

	/**
	 * blocks until the signal has been received that the experiment start time
	 * is now set
	 */
	public static void waitForExperimentStarttimeSet() {
		waitForSignalOnItem(InboxTopic.EXPERIMENT_STARTTIME_SET);
	}

	/**
	 * blocks until the preload trace has been received
	 */
	public static void waitForPreloadTraceReceived() {
		waitForSignalOnItem(InboxTopic.PRELOAD_TRACE_RECEIVED);
	}

	/**
	 * blocks until the warmup trace has been received
	 */
	public static void waitForWarmupTraceReceived() {
		waitForSignalOnItem(InboxTopic.WARMUP_TRACE_RECEIVED);
	}

	/**
	 * blocks until the experiment trace has been received
	 */
	public static void waitForExperimentTraceReceived() {
		waitForSignalOnItem(InboxTopic.EXPERIMENT_TRACE_RECEIVED);
	}

	/**
	 * blocks until the schema file has been received
	 */
	public static void waitForSchemaReceived() {
		waitForSignalOnItem(InboxTopic.SCHEMA_RECEIVED);
	}

	/**
	 * blocks until the oplist has been received
	 */
	public static void waitForOplistReceived() {
		waitForSignalOnItem(InboxTopic.OPLIST_RECEIVED);
	}

	/**
	 * blocks until the param list has been received
	 */
	public static void waitForParamListReceived() {
		waitForSignalOnItem(InboxTopic.PARAMLIST_RECEIVED);
	}

	/**
	 * blocks until the cust param list has been received
	 */
	public static void waitForCustParamListReceived() {
		waitForSignalOnItem(InboxTopic.CUST_PARAMLIST_RECEIVED);
	}
	
	/**
	 * blocks until the dbconnector config file has been received
	 */
	public static void waitForDbConnectorConfigReceived() {
		waitForSignalOnItem(InboxTopic.DB_CONFIG_RECEIVED);
	}
	

	public static void signalProceedToPreload() {
		wakeUpWaitingThreads(InboxTopic.PROCEED_TO_PRELOAD);
	}

	public static void signalExperimentStartTimeIsSet() {
		wakeUpWaitingThreads(InboxTopic.EXPERIMENT_STARTTIME_SET);
	}

	public static void signalPreloadTraceReceived() {
		wakeUpWaitingThreads(InboxTopic.PRELOAD_TRACE_RECEIVED);
	}

	public static void signalWarmupTraceReceived() {
		wakeUpWaitingThreads(InboxTopic.WARMUP_TRACE_RECEIVED);
	}

	public static void signalExperimentTraceReceived() {
		wakeUpWaitingThreads(InboxTopic.EXPERIMENT_TRACE_RECEIVED);
	}

	public static void signalSchemaReceived() {
		wakeUpWaitingThreads(InboxTopic.SCHEMA_RECEIVED);
	}

	public static void signalOplistReceived() {
		wakeUpWaitingThreads(InboxTopic.OPLIST_RECEIVED);
	}

	public static void signalParamlistReceived() {
		wakeUpWaitingThreads(InboxTopic.PARAMLIST_RECEIVED);
	}

	public static void signalCustParamlistReceived() {
		wakeUpWaitingThreads(InboxTopic.CUST_PARAMLIST_RECEIVED);
	}

	public static void signalConfigDetailsReceived() {
		wakeUpWaitingThreads(InboxTopic.CONFIG_DETAILS_RECEIVED);
	}

	public static void signalDbConfigFileReceived() {
		wakeUpWaitingThreads(InboxTopic.DB_CONFIG_RECEIVED);
	}

	private static class LockAndFieldTuple {
		private boolean value = false;
		private final Lock lock = new ReentrantLock();
		private final Condition condition = lock.newCondition();
	}

	private static enum InboxTopic {
		PROCEED_TO_PRELOAD, EXPERIMENT_STARTTIME_SET, PRELOAD_TRACE_RECEIVED, WARMUP_TRACE_RECEIVED, EXPERIMENT_TRACE_RECEIVED, SCHEMA_RECEIVED, OPLIST_RECEIVED, PARAMLIST_RECEIVED, CUST_PARAMLIST_RECEIVED, CONFIG_DETAILS_RECEIVED, DB_CONFIG_RECEIVED;
	}

}
