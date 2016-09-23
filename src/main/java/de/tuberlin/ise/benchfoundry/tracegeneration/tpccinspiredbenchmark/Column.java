package de.tuberlin.ise.benchfoundry.tracegeneration.tpccinspiredbenchmark;

import de.tuberlin.ise.benchfoundry.tracegeneration.tpccinspiredbenchmark.domain.Domain;

public class Column {

	private final String name;
	private final Domain domain;

	private Column(String name, Domain domain) {
		if(name == null || name.length() == 0)
			throw new IllegalArgumentException("Property name for class Column is mandatory: " +name);
		this.name = name;
		if(domain == null)
			throw new IllegalArgumentException("Property domain for class Column is mandatory!");
		this.domain = domain;
	}
	
	public static Column c(String name, Domain domain) {
		return new Column(name, domain);
	}
	
	public String getName() {
		return name;
	}

	public Domain getDomain() {
		return domain;
	}
	
}
