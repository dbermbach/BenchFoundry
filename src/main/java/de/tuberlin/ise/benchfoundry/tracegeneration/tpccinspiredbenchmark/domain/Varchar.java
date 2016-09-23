package de.tuberlin.ise.benchfoundry.tracegeneration.tpccinspiredbenchmark.domain;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class Varchar extends Domain {

	private final int m;
	private final char[] baseSet = {'A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','W','X','Y','Z'};
	private Map<String,String> leased;
	protected final Random generator = new Random();

	private char getRandomChar(){
		return baseSet[generator.nextInt(baseSet.length)];
	}
	
	public Varchar(boolean unique, int m) {
		super(unique);
		leased = new HashMap<>();
		if (m < 0 || m > 65535)
			throw new IllegalArgumentException("Length of Char Domain M must be 0 < M < 65535");
		this.m = m;
	}

	public int getM() {
		return m;
	}

	@Override
	public String getName() {
		return Varchar.class.getName().toUpperCase();
	}

	@Override
	public String nextInsertField() {
		String s = "";
		for(int i = 0; i < m; i++) {
			s += getRandomChar();
		}
		if(!leased.containsKey(s))
			leased.put(s, s);
		return "'" +s+ "'";
	}

	@Override
	public String nextReadField() {
		Object[] fields = leased.values().toArray();
		return "'" +((String) fields[generator.nextInt(fields.length)]).toString()+ "'";
	}

	@Override
	public String toDdl() {
		String ddl = "VARCHAR(" +m+ ")";
		if (unique)
			ddl += " UNIQUE";
		return ddl;
	}

}
