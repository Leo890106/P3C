/*
 * @author Van Quoc Phuong Huynh, FAW JKU
 * June, 2022
 *
 */

package core.prepr;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Abstract class for applying a specific adaptation on features list (selector list) of each attribute
 *
 */
public abstract class FeaturesSupporter {

	///////////////////////////////////////////////PROPERTIES SECTION//////////////////////////////////////////////
    public Set<String> except_attr_names;	// set of exceptional attributes
	
    public int row_count;				// the number of records in the dataset
    public int min_sup_count;			// minimum support count
    
    public int attr_count;				// the number of all attributes
    public int predict_attr_count;		// the number of predict attributes, the first attributes in the attributes list of the dataset
    public int target_attr_count = 1;	// the number of target attributes, the last attributes in the attributes list of the dataset

	public int numeric_attr_count;		// the number of numeric attributes
	public int distinct_value_count;	// the number of distinct values of attributes
	
	public int selector_count;			// predict_selector_count + target_selector_count
	public int predict_selector_count; 	// the number of frequent selectors from predict attributes
	public int target_selector_count;	// the number of selectors from target attributes
	
	
	/**
	 * List of attributes
	 */
	public List<Attribute> attributes;
	
	/**
	 * List of atom selectors in order
	 */
	public List<Selector> atom_selectors;
	
	/**
	 * <b>constructing_selectors</b> includes (in sequence):
	 * </br>1. FREQUENT ATOM selectors from predict attributes
	 * </br>2. FREQUENT ATOM selectors from target attributes which are in ascending order of support count
	 */
	public List<Selector> constructing_selectors;
	
    /**
     * Constructor
     */
    public FeaturesSupporter(){}
    
    /**
     * Make a specific adaptation on feature list (selector list) of each attribute
     * @param dr
     * @param lists the first list is the list of exceptional attribute names
     */
	public void adapt(DataReader dr, List<String>... lists){
    	if(dr == null) return;
    	
    	this.row_count = dr.getRowCount();
		this.min_sup_count = dr.getMinSupCount();
		
		this.attr_count = dr.getAttrCount();
		this.predict_attr_count = dr.getPredictAttrCount();
		this.target_attr_count = dr.getTargetAttrCount();
		this.numeric_attr_count = dr.getNumericAttrCount();
		
		this.except_attr_names = this.prepare_exceptional_attribute_names(dr.getAttributes(), lists[0]);
		
		// apply a specific adaptation on feature list (selector list) of each attribute
		this.make_adaptation(dr, lists);
		
		// update atom selector list
		this.atom_selectors = this.update_atom_selector_list(this.attributes);
				
		// update constructing selector list
		this.constructing_selectors = this.update_constructing_selector_list(atom_selectors);
		
		this.distinct_value_count = this.atom_selectors.size();
		this.selector_count = this.constructing_selectors.size();
		this.target_selector_count = dr.getTargetSelectorCount();
		this.predict_selector_count = this.selector_count - this.target_selector_count;
    }
	
	protected Set<String> prepare_exceptional_attribute_names(List<Attribute> attributes, List<String> attr_names){
		Set<String> except_attr_names;
		
		if(attr_names == null){
			// only the target attribute
			except_attr_names = new HashSet<String>();
			// add the target attribute at the last index
			except_attr_names.add(attributes.get(attributes.size()-1).name);
		}else{
			except_attr_names = new HashSet<String>(attr_names.size()+1);
			// add the target attribute at the last index
			except_attr_names.add(attributes.get(attributes.size()-1).name);
			for(String attr_name : attr_names){
				except_attr_names.add(attr_name);
			}
		}
		return except_attr_names;
	}
	
	protected List<Selector> update_atom_selector_list(List<Attribute> attributes){
		// get sorted list of selector from the predicting attributes
		List<Selector> atom_selector_list = new ArrayList<Selector>();
		for(int i=0; i<this.predict_attr_count; i++){
			Attribute attr = attributes.get(i);
			for(Selector selector : attr.distinct_values.values()){
				atom_selector_list.add(selector);
			}
		}
		
		IncreaseFreqComparator comparator = new IncreaseFreqComparator(); // increasing frequency
		atom_selector_list.sort(comparator);
		
		// get sorted list of selectors from the target attribute(s)
		List<Selector> target_atom_selector_list = new ArrayList<Selector>();
		for(int i=this.predict_attr_count; i<this.attr_count; i++){
			Attribute attr = attributes.get(i);
			for(Selector selector : attr.distinct_values.values()){
				target_atom_selector_list.add(selector);
			}
		}
		target_atom_selector_list.sort(comparator); // increasing frequency
		
		// concatenate the two lists
		atom_selector_list.addAll(target_atom_selector_list);
		
		// update distinctValueID of selectors
		int id = 0;
		for(Selector s : atom_selector_list){
			s.distinctValueID = id;
			id++;
		}
		
		return atom_selector_list;
	}
	
	protected List<Selector> update_constructing_selector_list(List<Selector> atom_selectors){
		List<Selector> constructing_selector_list = new ArrayList<Selector>(atom_selectors.size());
		
		// filter frequent selectors and update selector id 
		int id = 0;
		for(Selector selector : atom_selectors){
			if(selector.frequency >= this.min_sup_count){
				selector.selectorID = id;
				id++;
				constructing_selector_list.add(selector);
			}else{
				selector.selectorID = Selector.INVALID_ID;
			}
		}
		
		return constructing_selector_list;
	}
	
	// Must be implemented in subclasses
	protected abstract void make_adaptation(DataReader dr, List<String>... lists);
}
