/**
 * 
 */
package de.tuberlin.ise.benchfoundry.util;

/**
 * @author Dave
 *
 */
public interface PhaseChangeListener {

	/**
	 * will be invoked as soon as the phase changes.
	 * 
	 * @param oldPhase the previous phase
	 * @param newPhase the (now) current phase
	 */
	public void onPhaseChange(Phase oldPhase, Phase newPhase);

}
