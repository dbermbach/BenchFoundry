/**
 * 
 */
package de.tuberlin.ise.benchfoundry.physicalschema.model.columnstore;

/**
 * @author Dave
 *
 */
public enum CSColumnType {

	KEY,INDEX,REGULAR;

	/* (non-Javadoc)
	 * @see java.lang.Enum#toString()
	 */
	@Override
	public String toString() {
		switch(this){
		case KEY: return "key";
		case INDEX: return "ind";
		case REGULAR: return "reg";
		default: throw new RuntimeException("Update toString() method - obviously a new type was added!");
		}
	}
	
	
}
