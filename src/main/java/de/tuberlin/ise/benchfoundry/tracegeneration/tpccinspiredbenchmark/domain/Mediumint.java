package de.tuberlin.ise.benchfoundry.tracegeneration.tpccinspiredbenchmark.domain;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class Mediumint extends Domain {

	protected final int min;
	protected final int max;
	protected final boolean unsigned;
	protected final boolean zerofill;
	protected final Map<Integer, Integer> leased;
	protected final Random generator = new Random();

	public Mediumint(boolean unique, boolean unsigned, boolean zerofill) {
		super(unique);
		leased = new HashMap<>();
		if (unsigned) {
			this.unsigned = true;
			min = 0;
			max = 16777215;
		} else {
			this.unsigned = false;
			min = -8388608;
			max = 8388607;
		}
		if (zerofill)
			this.zerofill = true;
		else
			this.zerofill = false;
	}

	@Override
	public String getName() {
		return Mediumint.class.getName().toUpperCase();
	}

	@Override
	public String nextInsertField() {
		if (super.unique) {
			for (Integer i = min; i <= max; i++) {
				if (!leased.containsKey(i)) {
					leased.put(i, i);
					return i.toString();
				}
			}
			throw new RuntimeException("Cannot generate a new field for unique Domain class " + getName()
					+ " all fields of the domain have a lease.");
		} else {
			if (unsigned)
				return String.valueOf(generator.nextInt(8388607));
			else
				return String.valueOf(generator.nextInt(8388607) - 8388608);
		}
	}

	@Override
	public String nextReadField() {
		Object[] fields = leased.values().toArray();
		return ((Integer) fields[generator.nextInt(fields.length)]).toString();
	}

	@Override
	public String toDdl() {
		String ddl = "MEDIUMINT";
		if(unsigned)
			ddl += " UNSIGNED";
		if(zerofill)
			ddl += " ZEROFILL";
		if (unique)
			ddl += " UNIQUE";
		return ddl;
	}

}
