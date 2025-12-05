/*
 * @author Van Quoc Phuong Huynh, FAW JKU
 * June, 2022
 *
 */

package core.prepr;

import java.util.HashSet;
import java.util.List;
import java.util.Set;


/**
 * Support to eliminate features (e.g. 'false', 'no', '0') from attributes of a dataset.
 * </br></br>The example dataset with attributes A, B, C that their features {'f', '0'} will be removed.
 * <table border="0">
  <tr><td>A</td> <td>B</td> <td>C</td>  <td>Y</td> </tr>
  <tr><td>f</td> <td>t</td> <td>0</td> <td>y1</td></tr>
  <tr><td>t</td> <td>t</td> <td>0</td> <td>y2</td></tr>
  <tr><td>f</td> <td>f</td> <td>1</td> <td>y3</td></tr>
  <tr><td>f</td> <td>t</td> <td>1</td> <td>y3</td></tr>
  </table>
  
  </br></br> So the result will be, e.g.:</br>
  example0: <tr><td>f</td> <td>t</td> <td>0</td> <td>y1</td></tr> --> selectors record: <tr><td>B=t, </td><td>Y=y1</td></tr>
  </br>
  example2: <tr><td>f</td> <td>f</td> <td>1</td> <td>y3</td></tr> --> selectors record: <tr><td>C=1, </td><td>Y=y3</td></tr>
 */
public class FeaturesRemover extends FeaturesSupporter {    
    /**
     <ul>
    	<li>Create an instance of FeaturesRemover.</li> 
    	<li>Call method adapt(DataReader dr, List&ltString&gt exceptional_attr_names, List&ltString&gt rm_features) 
    	to eliminate features in 'rm_features' for attributes, except ones in list "exceptional_attr_names"</li>
    	<li>"exceptional_attr_names" == null, only the target attribute is the implicitly exceptional one.</li>
  	</ul>
     */
    public FeaturesRemover(){}
	

	@Override
	protected void make_adaptation(DataReader dr, List<String>... lists) {
		this.attributes = this.remove_selectors(dr.attributes, lists[0], lists[1]);
	}
	
	private List<Attribute> remove_selectors(List<Attribute> attributes,  
												List<String> except_attr_names,
												List<String> rm_features){
		// prepare a set of exceptional attributes
		Set<String> except_names;
		if(except_attr_names == null){
			// only the target attribute
			except_names = new HashSet<String>();
			// add the target attribute at the last index
			except_names.add(attributes.get(attributes.size()-1).name);
		}else{
			except_names = new HashSet<String>(except_attr_names.size()+1);
			// add the target attribute at the last index
			except_names.add(attributes.get(attributes.size()-1).name);
			for(String attr_name : except_attr_names){
				except_names.add(attr_name);
			}
		}
				
		for(Attribute attr : attributes){
			if(except_names.contains(attr.name)) continue;
			
			for(String rm_feature : rm_features){
				attr.distinct_values.remove(rm_feature);
			}
		}
		
		return attributes;
	}
}
