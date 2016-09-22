/**
 * 
 */
package de.tuberlin.ise.benchfoundry.logicalschema.model;

/**
 * 
 */

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.StatementVisitorAdapter;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;

/**
 * holds references to entities, describes a logical data schema.
 * 
 * 
 * 
 * @author Dave
 *
 */
public class DataSchema {

	/** singleton, initialized only */
	private final static DataSchema instance = new DataSchema();

	/** holds all entities, key is their name */
	private final Map<String, Entity> entities = new HashMap<String, Entity>();

	/** if true the data schema is complete and ready to be used */
	private boolean readyForUse = false;

	private final Pattern comment = Pattern.compile("^[ \t]*#.*$");

	/**
	 * 
	 * @return the singleton instance
	 */
	public static DataSchema getInstance() {
		return instance;
	}

	private DataSchema() {

	}

	/**
	 * reads all entities from the specified schema file
	 * 
	 * @param filename
	 * @throws IOException
	 * @throws ParseException
	 */
	public void addSchemaInputFile(String filename) throws IOException,
			ParseException {
		addSchemaInputFile(new FileInputStream(new File(filename)));
	}

	public static Statement parseQuery(String query) {
		Statement stmt = null;
		try {
			stmt = CCJSqlParserUtil.parse(query);
		} catch (JSQLParserException e) {
			e.printStackTrace();
		}
		return stmt;
	}

	class LogicalCreateTableStatementVisitor extends StatementVisitorAdapter {
		DataSchema _schema;

		public LogicalCreateTableStatementVisitor(DataSchema schema) {
			this._schema = schema;
		}

		@Override
		public void visit(CreateTable createTable) {
			String tableName = createTable.getTable().getName();

			List<String> strs = new ArrayList<String>();
			for (ColumnDefinition definition : createTable
					.getColumnDefinitions()) {
				strs.add(definition.getColumnName());
			}

			this._schema.createNewEntity(tableName,
					strs.toArray(new String[strs.size()]));
		}
	}

	/**
	 * @param fileInputStream
	 */
	private void addSchemaInputFile(FileInputStream is) throws IOException {
		String line;
		BufferedReader reader = new BufferedReader(new InputStreamReader(is));
		try {
			while ((line = reader.readLine()) != null) {
				if (comment.matcher(line).find() || line.trim().length() == 0)
					continue; // ignore comment lines

				String[] parts = line.split(":");
				int index = Integer.parseInt(parts[0]);

				/** the JSQLParser Statement generated from the input query */
				Statement stmt = parseQuery(parts[1]);
				stmt.accept(new LogicalCreateTableStatementVisitor(this));
			}
		} catch (IOException e) {
			e.printStackTrace();
			throw e;
		}
	}

	/**
	 * creates and adds a new entity.
	 * 
	 * @param entityName
	 *            name of the new entity, must not be null
	 * @param firstAttributeName
	 *            name of one of the attributes, must not be null
	 * @param otherAttributeNames
	 *            name of other attributes, optional but don't add null values
	 * @return true if successfully added, false otherwise.
	 * @throws RuntimeException
	 *             if the schema has not been set ready for use.
	 */
	public boolean createNewEntity(String entityName, String... attributeNames) {
		if (entityName == null || attributeNames == null
				|| entities.containsKey(entityName)) {
			return false;
		}
		Entity e = new Entity(entityName);
		for (String s : attributeNames) {
			if (s != null) {
				e.addAttribute(s);
			} else {
				return false;
			}
		}
		entities.put(entityName, e);
		return true;
	}

	/**
	 * creates a new relationship between two entities. Comments use example of
	 * Customer and Order.
	 * 
	 * @param firstEntity
	 *            e.g., Customer
	 * @param secondEntity
	 *            e.g., Order
	 * @param firstAttribute
	 *            e.g., ordersByThisCustomer
	 * @param secondAttribute
	 *            e.g., customerOfThisOrder
	 * @param firstCardinality
	 *            e.g., ONE since there is only one customer per order
	 * @param secondCardinality
	 *            e.g., MANY since there may be more than one order per customer
	 * @return true if the relationship could be created (i.e., no null
	 *         parameters, entities did exist, attributes did exist,relationship
	 *         was not already specified):
	 * @throws RuntimeException
	 *             if the schema has not been set ready for use. or: if adding
	 *             the relationship fails in between and the clean-up process
	 *             fails as well. The latter should never happen, though.
	 */
	public boolean addRelationship(String firstEntity, String secondEntity,
			String firstAttribute, String secondAttribute,
			RelationshipType firstCardinality,
			RelationshipType secondCardinality) {
		if (firstEntity == null || secondEntity == null
				|| firstAttribute == null || secondAttribute == null
				|| firstCardinality == null || secondCardinality == null
				|| !entities.containsKey(firstEntity)
				|| !entities.containsKey(secondEntity))
			return false;
		Entity first = entities.get(firstEntity), second = entities
				.get(secondEntity);
		boolean success = first.addRelationship(firstAttribute, second,
				secondCardinality, firstCardinality);
		if (!success)
			return false;
		success = second.addRelationship(secondAttribute, first,
				firstCardinality, secondCardinality);
		if (!success) {
			// undo changes in first
			first.dropRelationship(firstAttribute);
			return false;
		}
		return true;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		StringBuffer res = new StringBuffer("Logical data schema:");
		for (Entity e : entities.values())
			res.append("\n\t" + e);
		return res.toString();
	}

	/**
	 * retrieves an attribute by its name
	 * 
	 * @param attributeName
	 *            name of the attribute
	 * @param entityName
	 *            name of the surrounding entity
	 * @return an attribute or null if no entry was found for either of the
	 *         parameters
	 * @throws RuntimeException
	 *             if the schema has not been set ready for use.
	 */
	public Attribute getAttributeByName(String attributeName, String entityName) {
		Entity e = entities.get(entityName);
		if (e != null)
			return e.getAttributeByName(attributeName);
		return null;
	}

	/**
	 * 
	 * @param entityName
	 * @return null if the specified entity does not exist or a set of the
	 *         specified entity's attributes
	 */
	public Set<Attribute> getAttributesForEntity(String entityName) {
		Entity e = entities.get(entityName);
		if (e == null)
			return null;
		return e.getAttributes();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return entities.hashCode();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!(obj instanceof DataSchema))
			return false;
		DataSchema other = (DataSchema) obj;

		return this.entities.equals(other.entities);
	}

	/**
	 * @return true if the data schema is complete and can be used
	 */
	public boolean isReadyForUse() {
		return this.readyForUse;
	}

	/**
	 * after calling this method, no additional entities or relationships can be
	 * added
	 * 
	 * 
	 */
	public void setReadyForUse() {
		this.readyForUse = true;
	}

	/**
	 * 
	 * @param name
	 * @return the {@link Entity} with the specified name or null if no such
	 *         mapping exists
	 */
	public Entity getEntityForName(String name) {
		return entities.get(name);
	}

}
