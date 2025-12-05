/*
 * @author Van Quoc Phuong Huynh, FAW JKU
 * June, 2022
 *
 */

package core.prepr;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;


/**
 * Add negative features (selectors) for attributes (except some specified attributes).
 * See the below example for the way it works.</br></br>
 An example dataset, attribute C and the target attribute Y do not support negative features
  <table border="0">
  <tr><td>A</td>  <td>B</td>  <td>C</td>  <td>Y</td> </tr>
  <tr><td>a1</td> <td>b1</td> <td>c2</td> <td>y1</td></tr>
  <tr><td>a2</td> <td>b1</td> <td>c2</td> <td>y2</td></tr>
  <tr><td>a3</td> <td>b2</td> <td>c1</td> <td>y3</td></tr>
  <tr><td>a3</td> <td>b1</td> <td>c1</td> <td>y3</td></tr>
  </table>
  
  </br>The attributes and the corresponding selectors
  </br>Attribute A: 
  <ul>
    <li>positive selectors: (A=a1), (A=a2), (A=a3)</li> 
    <li>negative selectors: !(A=a1), !(A=a2), !(A=a3)</li>
  </ul>
  </br>Attribute B: 
  <ul>
    <li>positive selectors: (B=b1), (B=b2)</li> 
    <li>negative selectors: !(B=b1), !(B=b2)</li>
  </ul>
  </br>Attribute C: 
  <ul>
    <li>positive selectors: (C=c1), (C=c2)</li> 
    <li>negative selectors: not supported</li>
  </ul>
  </br>Attribute Y: 
  <ul>
    <li>positive selectors: (Y=y1), (Y=y2)</li> 
    <li>negative selectors: not supported</li>
  </ul>
  
  The first example: <tr><td>a1</td> <td>b1</td> <td>c2</td> <td>y1</td></tr> 
  will be corresponding to selectors list </br></br>
  <tr><td>(A=a1), </td> <td>!(A=a2), </td> <td>!(A=A3), </td> <td>(B=b1), </td> <td>!(B=b2), </td> <td>(C=c2), </td> <td>Y=y1</td></tr>
  
  <br></br>
  
  that has the same logic as the corresponding one-hot-encoding transforming
  <table border="0">
  <tr><td>(A=a1)</td> <td>(A=a2)</td> <td>(A=A3)</td> <td>(B=b1)</td> <td>(B=b2)</td> <td>C</td> <td>Y</td></tr>
  <tr><td>t</td> <td>f</td> <td>f</td> <td>t</td> <td>f</td> <td>c2</td> <td>y1</td></tr>
  </table>
 */
public class NegFeaturesAdder extends FeaturesSupporter {    
	/**
	<ul>
    	<li>Create an instance of NegFeaturesAdder.</li> 
    	<li>Call method adapt(DataReader dr, List&ltString&gt exceptional_attr_names) to add negative features for attributes,
    	except ones in list "exceptional_attr_names"</li>
    	<li>"exceptional_attr_names" == null, only the target attribute is the implicitly exceptional one.</li>
  	</ul>
     */
    public NegFeaturesAdder(){}
    	
	/**
	 * Convert input record of values (an example) to the corresponding record of selectorIDs of both positive and negative selectors
	 * @param value_record
	 * @param id_buffer
	 * @return record of selectorIDs of negative and positive selectors corresponding the input exammple
	 */
	public int[] convert_values_to_selectorIDs(String[] value_record, int[] id_buffer){
		int count=0;
		Selector s;
		
		for(int i=0; i<value_record.length; i++){
			Attribute attr = this.attributes.get(i);
			s = attr.getSelector(value_record[i]);
			
			if(s == null) continue;
			
			// add id of the positive selector 
			if(s.selectorID != Selector.INVALID_ID){
				id_buffer[count] = s.selectorID;
				count++;
			}
			
			// add id of negative selectors of attribute supporting negative features
			if(this.except_attr_names.contains(attr.name)) continue;
			
			String positive_value = s.distinctValue;
			for(Selector selector : attr.distinct_values.values()){
				if(selector.distinctValue.charAt(0) == '!' && 
					!selector.distinctValue.startsWith(positive_value, 1) &&
					selector.selectorID != Selector.INVALID_ID){
					
					id_buffer[count] = selector.selectorID;
					count++;
				}
			}
		}
		
		int[] id_record = new int[count];
		System.arraycopy(id_buffer, 0, id_record, 0, count);
		
		return id_record;
	}

	@Override
	protected void make_adaptation(DataReader dr, List<String>... lists) {
		// add negative selectors for attributes, except attributes in 'this.except_attr_names'
		this.attributes = this.add_negative_selectors(dr.getAttributes(), this.except_attr_names);
	}
	
	private List<Attribute> add_negative_selectors(List<Attribute> attributes, Set<String> noNegFeature_attrNames){
		List<Selector> neg_selectors = new ArrayList<Selector>();
		
		for(Attribute attr : attributes){
			// do not add negative selectors for attributes in noNegFeature_attrNames
			if(noNegFeature_attrNames.contains(attr.name)) continue;
			
			for(Selector selector : attr.distinct_values.values()){
				// create the negative selector of the selector, initialize it with the selector's properties
				Selector neg_selector = selector.clone();
				neg_selectors.add(neg_selector);
				
				// update properties for the negative selector
				neg_selector.condition = "!" + neg_selector.condition;
				neg_selector.distinctValue = "!" + neg_selector.distinctValue;
				// the frequency of the negative selector is the sum of other positive selector
				int feq = 0;
				for(Selector s : attr.distinct_values.values()){
					if (s != selector) feq += s.frequency;
				}
				neg_selector.frequency = feq;
				// distinctValueID, selectorID of all selectors (all positive and negative) must be re-calculated later
			}
		}
		
		for(Selector neg_selector : neg_selectors){
			attributes.get(neg_selector.attributeID).distinct_values.put(neg_selector.distinctValue, neg_selector);
		}
		
		return attributes;
	}
}
