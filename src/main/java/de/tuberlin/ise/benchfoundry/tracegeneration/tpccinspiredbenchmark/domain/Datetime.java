package de.tuberlin.ise.benchfoundry.tracegeneration.tpccinspiredbenchmark.domain;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class Datetime extends Domain {

	private final Random generator = new Random();
	private final Map<String, String> leased;
	
	public Datetime(boolean unique) {
		super(unique);
		leased = new HashMap<>();
	}

	@Override
	public String getName() {
		return Datetime.class.getName().toUpperCase();
	}

	@Override
	public String nextInsertField() {
		String s = "2016-";
		s += (generator.nextInt(11)+1);
		s += "-";
		s += (generator.nextInt(29)+1);
		/** TODO We need a way to deal with the ":"
		s += " ";
		s += (generator.nextInt(23)+1);
		s += ":";
		s += (generator.nextInt(59)+1);
		s += ":";
		s += (generator.nextInt(60)+1);
		*/
		if(!leased.containsKey(s))
			leased.put(s, s);
		return s;
	}

	@Override
	public String nextReadField() {
		Object[] fields = leased.values().toArray();
		return ((String) fields[generator.nextInt(fields.length)]).toString();
	}

	@Override
	public String toDdl() {
		String ddl = "DATETIME";
		if( unique)
			ddl += " UNIQUE";
		return ddl;
	}

}
