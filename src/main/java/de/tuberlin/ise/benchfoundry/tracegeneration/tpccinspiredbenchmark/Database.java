package de.tuberlin.ise.benchfoundry.tracegeneration.tpccinspiredbenchmark;

import java.util.ArrayList;
import java.util.List;

public class Database {

	private final List<Table> tables;
	private final String name;
	
	private Database(String name) {
		this.tables = new ArrayList<>();
		if(name == null || name.length() == 0)
			throw new IllegalArgumentException("The field name for class Database is mandatory!");
		this.name = name;
	}
	
	public static Database instance(String name){
		return new Database(name);
	}

	public List<Table> getTables() {
		return tables;
	}

	public String getName() {
		return name;
	}
	
	public Database addTable(Table table) {
		tables.add(table);
		return this;
	}
	
}
