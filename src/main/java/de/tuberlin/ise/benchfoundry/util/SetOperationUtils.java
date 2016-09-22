/**
 * 
 */
package de.tuberlin.ise.benchfoundry.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author Dave
 *
 */
public class SetOperationUtils {

	/**
	 * calculates all potential subsets that can be constructed from the set of
	 * objects specified by the parameter inputSet. The empty set and the full
	 * set are included in the result.
	 * 
	 * @param <T>
	 *            the type of the object
	 * 
	 * @param inputSet
	 *            must not have a length of zero
	 * @return all potential subsets
	 */
	public static <T> Set<Set<T>> getAllSubSets(T[] inputSet) {
		return getAllSubSets(inputSet, inputSet.length - 1);
	}

	/**
	 * 
	 * calculates all potential subsets that can be constructed from the set of
	 * objects specified by the inputSet array between the index positions 0 and
	 * maxPos. The empty set and the full set are included in the result.
	 * 
	 * @param <T>
	 *            the type of the object in the result
	 * 
	 * @param inputSet
	 *            must not have a length of zero
	 * @param maxPos
	 *            will default to attributes.length-1 if it is greater than said
	 *            value.
	 * @return all potential subsets
	 */
	private static <T> Set<Set<T>> getAllSubSets(T[] inputSet, int maxPos) {
		Set<Set<T>> permutations = new HashSet<Set<T>>();
		maxPos = Math.min(maxPos, inputSet.length - 1);
		if (maxPos < 0) {
			// don't add elements
		} else if (maxPos == 0) {
			permutations.add(new HashSet<T>());
			HashSet<T> tmp = new HashSet<T>();
			tmp.add(inputSet[0]);
			permutations.add(tmp);
		} else {
			Set<Set<T>> perms = getAllSubSets(inputSet, maxPos - 1);
			permutations.addAll(perms);
			for (Set<T> set : perms) {
				HashSet<T> tmp = new HashSet<T>(set);
				tmp.add(inputSet[maxPos]);
				permutations.add(tmp);
			}
		}
		return permutations;
	}

	/**
	 * 
	 * @param input
	 * @return the set of all two-item combinations that can be built based on
	 *         the input set
	 */
	public static <T> Set<Set<T>> getAllSizeTwoSubsets(Set<T> input) {
		Set<Set<T>> result = new HashSet<Set<T>>();
		for (T entry : input) {
			for (T other : input) {
				if (entry != other) {
					Set<T> combination = new HashSet<T>();
					combination.add(other);
					combination.add(entry);
					result.add(combination);
				}
			}
		}
		return result;
	}

	/**
	 * calculates the cartesian product of the provided parameters. Results are
	 * guaranteed to contain exactly one entry from each of the inner lists.
	 * 
	 * @param input
	 * @return
	 */
	public static <T> List<List<T>> getCartesianProduct(
			List<? extends List<T>> input) {
		List<List<T>> result = new ArrayList<List<T>>();
		CartesianProductCounter cpc = new CartesianProductCounter(input);
		int[] vector = cpc.getNextVector();
		while (vector != null && !cpc.isDone()) {
			List<T> combination = new ArrayList<T>();
			result.add(combination);
			for (int i = 0; i < vector.length; i++) {
				List<T> list = input.get(i);
				combination.add(list.get(vector[i]));
			}
			vector = cpc.getNextVector();
		}
		return result;
	}

	/**
	 * a counter for numbers where every single digit may come from a different
	 * number system (e.g., first number could be octal, second decimal, third
	 * hexadecimal, etc.). Uses dimensions of lists for this counter and can be
	 * used to calculate the cartesian product of an unknown number of vectors
	 * with an unspecified dimension number (which may be different for each
	 * vector)
	 * 
	 * @author Dave
	 *
	 */
	private static class CartesianProductCounter {

		private final int[] counter;
		private final int[] maxValues;
		private boolean done = false;
		private boolean oneMore = false;

		private <T> CartesianProductCounter(List<? extends List<T>> input) {
			counter = new int[input.size()];
			maxValues = new int[input.size()];
			int counter = 0;
			for (List<T> list : input) {
				maxValues[counter++] = list.size() - 1;
			}
		}

		/**
		 * 
		 * @return the next number or null if the counter has reached its
		 *         maximum.
		 */
		private int[] getNextVector() {
			if (oneMore && !done) {
				done = true;
				return Arrays.copyOf(counter, counter.length);
			}
			if (done)
				return null;
			int[] result = Arrays.copyOf(counter, counter.length);
			int incrementablePos = -1;
			// find the first position which can be incremented
			for (int i = counter.length - 1; i >= 0; i--) {
				if (counter[i] < maxValues[i]) {
					incrementablePos = i;
					break;
				}
			}
			if (incrementablePos == -1) {
				// all positions in counter have reached their respective max
				// value
				oneMore = true;
				return result;
			}
			// increment the position and set all greater index positions to
			// zero
			counter[incrementablePos]++;
			for (int i = incrementablePos + 1; i < counter.length; i++) {
				counter[i] = 0;
			}
			return result;
		}

		/**
		 * 
		 * @return whether the last number has already been returned
		 */
		private boolean isDone() {
			return done;
		}
	}

	/**
	 * intersects the sets setA and setB
	 * 
	 * @param setA
	 * @param setB
	 * @return a set containing all elements that are contained in both setA and
	 *         setB
	 */
	public static <T> Set<T> intersect(Set<T> setA, Set<T> setB) {
		if (setA.containsAll(setB))
			return setB;
		if (setB.containsAll(setA))
			return setA;
		Set<T> intersection = new HashSet<T>();
		for (T t : setA) {
			if (setB.contains(t))
				intersection.add(t);
		}
		return intersection;
	}

}
