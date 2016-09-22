/**
 * 
 */
package de.tuberlin.ise.benchfoundry.util;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This class provides notifications for phase changes to any interested party.
 * 
 * @author Dave
 *
 */
public class PhaseManager {

	private static final Logger LOG = LogManager.getLogger(PhaseManager.class);
	
	/** the current phase */
	private Phase currentPhase = Phase.INIT;

	/**
	 * holds all registered listeners that will be notified of phase changes,
	 * only the value is used
	 */
	private Map<Integer, PhaseChangeListener> listeners = new ConcurrentHashMap<>();

	/** singleton */
	private static PhaseManager instance = new PhaseManager();

	/**
	 * 
	 * @return the singleton instance
	 */
	public static PhaseManager getInstance() {
		return instance;
	}

	private PhaseManager() {
	}

	/**
	 * moves to the next phase or does nothing if the last phase has already
	 * been reached
	 */
	public synchronized void changePhase() {
		if (this.currentPhase == Phase.values()[Phase.values().length - 1])
			return;
		Phase newPhase = Phase.values()[this.currentPhase.ordinal() + 1];
		changePhaseTo(newPhase);
	}

	/**
	 * changes to the specified phase
	 * 
	 * @param phase
	 *            target phase
	 */
	public synchronized void changePhaseTo(Phase phase) {
		Phase old = this.currentPhase;
		this.currentPhase = phase;
		for (PhaseChangeListener pcl : listeners.values())
			pcl.onPhaseChange(old, currentPhase);
		LOG.info("Changed phase from " + old +" to " + phase);
	}

	/**
	 * registers the specified listener. Please, note that it may not be
	 * immediately notified of a concurrent phase change.
	 * 
	 * @param pcl
	 */
	public void registerListener(PhaseChangeListener pcl) {
		listeners.put(pcl.hashCode(), pcl);
	}

	/**
	 * @return the currentPhase
	 */
	public Phase getCurrentPhase() {
		return this.currentPhase;
	}

}
