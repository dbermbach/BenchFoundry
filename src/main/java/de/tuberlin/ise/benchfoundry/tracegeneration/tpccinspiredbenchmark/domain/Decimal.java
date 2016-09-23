package de.tuberlin.ise.benchfoundry.tracegeneration.tpccinspiredbenchmark.domain;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class Decimal extends Domain {

	private int m = 10;
	private int d = 0;
	private final Map<Long, Long> leased;
	protected final Random generator = new Random();

	public Decimal(boolean unique, int precision, int scale) {
		super(unique);
		leased = new HashMap<>();
		if (precision < 1 || precision > 65)
			throw new IllegalArgumentException("Precision M must be 0 < M < 65");
		if (scale < 0 || scale > 30 || scale > precision)
			throw new IllegalArgumentException("Scale D for precision M must be 0 <= D < 30 && D <= M");
		this.m = precision;
		this.d = scale;
	}

	public Decimal(boolean unique, int precision) {
		super(unique);
		leased = new HashMap<>();
		if (precision < 1 || precision > 65)
			throw new IllegalArgumentException("Precision M must be 0 < M < 65");
		this.m = precision;
	}

	public Long getMin() {
		return new Long(0);
	}

	public Long getMax() {
		return new Long(10 ^ m);
	}

	public int getPrecision() {
		return m;
	}

	public int getScale() {
		return d;
	}

	@Override
	public String getName() {
		return Decimal.class.getName().toUpperCase();
	}

	@Override
	public String nextInsertField() {
		if(super.unique) {
			for(Long i = getMin(); i <= getMax(); i++) {
				if(!leased.containsKey(i)) {
					leased.put(i, i);
					if(d == 0)
						return String.valueOf(i);
					return String.valueOf(i/d);
				}
			}
			throw new RuntimeException("Cannot generate a new field for unique Domain class " +getName()+ " all fields of the domain have a lease.");
		}
			
		String s = "";
		for( int i = 0; i < m; i++) {
				if(i == (m-d))
					s += ".";
				s += generator.nextInt(10);
		}
		if(!leased.containsKey(new Long(s.replace(".", ""))))
			leased.put(new Long(s.replace(".", "")), new Long(s.replace(".", "")));
		return s;
	}

	@Override
	public String nextReadField() {
		Object[] fields = leased.values().toArray();
		return ((Long) fields[generator.nextInt(fields.length)]).toString();
	}

	@Override
	public String toDdl() {
		String ddl = "DECIMAL(" +m+ ", " +d+ ")";
		if( unique )
			ddl += " UNIQUE";
		return ddl;
	}

}
