/**
 * 
 */
package de.tuberlin.ise.benchfoundry.physicalschema.model.columnstore.schemagen;

import java.lang.reflect.Field;
import java.util.Map.Entry;
import java.util.Set;

import junit.framework.TestCase;
import de.tuberlin.ise.benchfoundry.logicalschema.model.Attribute;
import de.tuberlin.ise.benchfoundry.logicalschema.model.DataSchema;
import de.tuberlin.ise.benchfoundry.logicalschema.model.Entity;
import de.tuberlin.ise.benchfoundry.logicalschema.model.RelationshipType;
import de.tuberlin.ise.benchfoundry.logicalschema.queries.JoinQuery;
import de.tuberlin.ise.benchfoundry.logicalschema.queries.LogicalQuery;
import de.tuberlin.ise.benchfoundry.logicalschema.queries.LogicalQueryRegistry;
import de.tuberlin.ise.benchfoundry.logicalschema.queries.SelectQuery;
import de.tuberlin.ise.benchfoundry.physicalschema.model.columnstore.CSTable;
import de.tuberlin.ise.benchfoundry.physicalschema.model.columnstore.schemagen.CSSchemaOption;
import de.tuberlin.ise.benchfoundry.physicalschema.model.columnstore.schemagen.CSSchemaOptionGenerator;

/**
 * @author Dave
 *
 */
public class CSSchemaOptionGeneratorTest extends TestCase {

	private static String CUSTOMER = "CSSOGT_customer";
	private static String FNAME = "fname";
	private static String LNAME = "lname";
	private static String ID = "id";
	private static String ADDRESS = "address";
	private static String AGE = "age";
	
	private static String ORDERS = "CSSOGT_orders";
	private static String ITEM = "item";
	private static String QTY = "qty";
	private static String ORDER_ID = "order_id";
	private static String CUST_ID = "cust_id";
	
	/*
	 * (non-Javadoc)
	 * 
	 * @see junit.framework.TestCase#setUp()
	 */
	protected void setUp() throws Exception {
		super.setUp();
		// build schema
		DataSchema.getInstance().createNewEntity(CUSTOMER, FNAME, LNAME, ID, ADDRESS, AGE, ORDERS);
		DataSchema.getInstance().createNewEntity(ORDERS, ITEM, QTY, ORDER_ID, CUST_ID);
		DataSchema.getInstance().addRelationship(CUSTOMER, ORDERS, ID, CUST_ID, RelationshipType.MANY, RelationshipType.ONE);
		DataSchema.getInstance().setReadyForUse();

		// create some queries
		Field indexed = LogicalQuery.class.getDeclaredField("filterAttributes");
		indexed.setAccessible(true);
		Field results = LogicalQuery.class.getDeclaredField("affectedAttributes");
		results.setAccessible(true);
		Field affEnt = LogicalQuery.class.getDeclaredField("affectedEntities");
		affEnt.setAccessible(true);
		LogicalQueryRegistry lqr = LogicalQueryRegistry.getInstance();

		LogicalQuery query1 = new SelectQuery("select fname, lname from CSSOGT_customer where id=5", 1);
		lqr.registerQuery(query1);
		Set<Attribute> filterAttributes = (Set<Attribute>) indexed.get(query1);
		filterAttributes.add(DataSchema.getInstance().getAttributeByName(ID, CUSTOMER));
		Set<Attribute> affectedAttributes = (Set<Attribute>) results.get(query1);
		affectedAttributes.add(DataSchema.getInstance().getAttributeByName(FNAME, CUSTOMER));
		affectedAttributes.add(DataSchema.getInstance().getAttributeByName(LNAME, CUSTOMER));
		Set<Entity> affectedEntities = (Set<Entity>) affEnt.get(query1);
		affectedEntities.add(DataSchema.getInstance().getEntityForName(CUSTOMER));

		LogicalQuery query2 = new JoinQuery(
				"select orders.item from CSSOGT_customer,CSSOGT_orders where"
						+ " CSSOGT_customer.fname=? and CSSOGT_customer.lname=? and CSSOGT_customer.id=CSSOGT_orders.cust_id",
				2);
		lqr.registerQuery(query2);
		filterAttributes = (Set<Attribute>) indexed.get(query2);
		filterAttributes.add(DataSchema.getInstance().getAttributeByName(ID, CUSTOMER));
		filterAttributes.add(DataSchema.getInstance().getAttributeByName(FNAME, CUSTOMER));
		filterAttributes.add(DataSchema.getInstance().getAttributeByName(LNAME, CUSTOMER));
		filterAttributes.add(DataSchema.getInstance().getAttributeByName(CUST_ID, ORDERS));
		affectedAttributes = (Set<Attribute>) results.get(query2);
		affectedAttributes.add(DataSchema.getInstance().getAttributeByName(ITEM, ORDERS));
		affectedEntities = (Set<Entity>) affEnt.get(query2);
		affectedEntities.add(DataSchema.getInstance().getEntityForName(CUSTOMER));
		affectedEntities.add(DataSchema.getInstance().getEntityForName(ORDERS));
		
		System.out.println(DataSchema.getInstance());
		System.out.println(lqr);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see junit.framework.TestCase#tearDown()
	 */
	protected void tearDown() throws Exception {
		super.tearDown();
	}

	/**
	 * Test method for
	 * {@link de.tuberlin.ise.benchfoundry.physicalschema.model.columnstore.schemagen.CSSchemaOptionGenerator#generateSchemaOptions()}
	 * .
	 */
	public void testGenerateSchemaOptions() {
		CSSchemaOptionGenerator gen = new CSSchemaOptionGenerator();
		gen.generateSchemaOptions();
		System.out.println("Table Variants:");
		for(Entry<Integer, Set<CSTable>> entry : gen.getTableVariants().entrySet()){
			System.out.println("\tVariants for query " + entry.getKey());
			for(CSTable tbl : entry.getValue()){
				System.out.println("\t\t"+ tbl.toSimpleString());
			}
		}
		System.out.println("Schema Options:");
		for(CSSchemaOption option: gen.getSchemaOptions()){
			System.out.println(option);
		}
		fail("Not yet implemented");
	}

}
