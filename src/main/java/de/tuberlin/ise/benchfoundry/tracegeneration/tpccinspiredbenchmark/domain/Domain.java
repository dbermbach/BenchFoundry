package de.tuberlin.ise.benchfoundry.tracegeneration.tpccinspiredbenchmark.domain;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public abstract class Domain {

	protected final boolean unique;

	public abstract String getName();

	public abstract String nextInsertField();
	
	public abstract String toDdl();

	// public abstract String nextDeleteField();
	
	public abstract String nextReadField();
	
	protected Domain(boolean unique) {
		this.unique = unique;
	}

}
