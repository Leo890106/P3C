/*
 * @author Van Quoc Phuong Huynh, FAW JKU
 *
 */

package core.prepr;


public class Selector {
	public static final int INVALID_ID = -1;
	
	/**
	 * Attribute ID
	 */
	public int attributeID = INVALID_ID;
	
	/**
	 * Attribute name
	 */
	public String attributeName;
	
	/**
	 * The support count of the selector/distinct value in a data set
	 */
	public int frequency = 0;
	
	/**
	 * A distinct value of an attribute determined by <b>attributeID</b>
	 */
	public String distinctValue;
	
	/**
	 * Distinct value ID
	 */
	public int distinctValueID = Selector.INVALID_ID;
	
	/**
	 * String presentation of a selection condition, e.g. A=a1
	 */
	public String condition;
	
	/**
	 * Selector ID, is also the index of the selector in a list of frequent selectors.
	 * </br>Only frequent selectors have a valid selector id.
	 */
	public int selectorID = INVALID_ID;
	
	/**
	 * Build a selector
	 * @param attribute_id the index of the attribute in the attribute list
	 * @param attribute_name attribute name
	 * @param distinctValue a distinct value of the attribute
	 * @param frequency initialized value of frequency
	 */
	public Selector(int attribute_id, String attribute_name, String distinctValue, int frequency){
		this.attributeID = attribute_id;
		this.attributeName = attribute_name;
		this.distinctValue = distinctValue;
		this.condition = new StringBuilder(100).append(this.attributeName)
				.append('=').append(this.distinctValue).toString();
		this.frequency = frequency;
	}
	
	private Selector(int attribute_id, 
					String attribute_name, 
					int frequency, 
					String condition, 
					int selectorID,
					String distinct_value,
					int distinctValueID){
		this.attributeID = attribute_id;
		this.attributeName = attribute_name;
		this.frequency = frequency;
		this.condition = condition;
		this.selectorID = selectorID;
		this.distinctValue = distinct_value;
		this.distinctValueID = distinctValueID;
	}

	/**
	* Make a copy of this object
	*/
	public Selector clone(){
		return new Selector(this.attributeID, 
							this.attributeName, 
							this.frequency, 
							this.condition, 
							this.selectorID,
							this.distinctValue,
							this.distinctValueID);
	}
}
