/**
 * 
 */
package de.tuberlin.ise.benchfoundry.tracegeneration.consistencybenchmark;

import java.math.BigInteger;
import java.util.HashSet;
import java.util.Scanner;

/**
 * @author Dave
 *
 */
public class Utils {

	/**
	 * sets the class variable benchmarkDurationInMs with the parsed value of
	 * inputRead in ms
	 * 
	 * @param inputRead
	 *            a String in the format 1w 2d 3h 4m for a test run that takes
	 *            one week, 2 days and 3 hours and 4 minutes
	 * @return the value in ms if parsing was successful, -1 otherwise
	 */
	public static long parseDuration(String inputRead) {
		long benchmarkDurationInMs = 0;
		try {
			String[] splits = inputRead.split("\\s");
			for (String s : splits) {
				if (s.trim().length() == 0)
					continue;
				if (s.endsWith("w")) {
					int number = Integer
							.parseInt(s.substring(0, s.length() - 1));
					benchmarkDurationInMs += 1000 * 60 * 60 * 24 * 7 * number;
				} else if (s.endsWith("d")) {
					int number = Integer
							.parseInt(s.substring(0, s.length() - 1));
					benchmarkDurationInMs += 1000 * 60 * 60 * 24 * number;
				} else if (s.endsWith("h")) {
					int number = Integer
							.parseInt(s.substring(0, s.length() - 1));
					benchmarkDurationInMs += 1000 * 60 * 60 * number;
				} else if (s.endsWith("m")) {
					int number = Integer
							.parseInt(s.substring(0, s.length() - 1));
					benchmarkDurationInMs += 1000 * 60 * number;
				} else {
					System.out.println("Invalid input! Retry.");
					return -1;
				}
			}
		} catch (RuntimeException re) {
			System.out.println("Invalid input! Retry.");
			return -1;
		}

		return benchmarkDurationInMs;
	}

	/**
	 * @param in
	 * @return
	 */
	public static long readLong(Scanner in) {
		while (true) {
			if (in.hasNextLong())
				return in.nextLong();
			else {
				System.out.println("Invalid input (" + in.next() + "), retry.");
			}
		}

	}

	/**
	 * @param in
	 * @return
	 */
	public static double readProbability(Scanner in) {
		while (true) {
			if (in.hasNextDouble()) {
				double temp = in.nextDouble();
				if (temp >= 0 && temp <= 1) {
					return temp;
				} else {
					System.out.println("Invalid input (" + temp + "), retry.");
				}
			} else {
				System.out.println("Invalid input (" + in.next() + "), retry.");
			}
		}

	}

	/**
	 * calculates the probability of reaching all replicas when X readers
	 * concurrently issue a read request to a randomly selected replica.
	 * 
	 * @param replicationFactor
	 * @param numberOfReaders
	 * @return
	 */
	public static double calculateProbabilityOfReachingAllReplicas(
			int replicationFactor, int numberOfReaders) {
		int m = numberOfReaders, n = replicationFactor;
		if (m < n)
			return 0;
		double result = 0;
		double nPowM = Math.pow(n, m);
		for (int i = 1; i < n; i++) {
			if (i % 2 == 0) {
				result -= binomialCoefficient(n, i) * Math.pow(n - i, m)
						/ nPowM;
			} else {
				result += binomialCoefficient(n, i) * Math.pow(n - i, m)
						/ nPowM;
			}
		}
		return 1 - result;
	}

	/**
	 * 
	 * @param n
	 * @param k
	 * @return n choose k
	 */
	public static long binomialCoefficient(int n, int k) {
		BigInteger nBI = BigInteger.valueOf(n), kBI = BigInteger.valueOf(k);
		BigInteger res = BigInteger.ONE;
		for (BigInteger i = BigInteger.ONE; i.compareTo(kBI) <= 0; i = i
				.add(BigInteger.ONE)) {
			res = res.multiply(nBI).divide(i);
			nBI = nBI.subtract(BigInteger.ONE);
		}
		return res.longValue();
	}

	
}
