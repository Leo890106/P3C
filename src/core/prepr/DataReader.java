/*
 * @author Van Quoc Phuong Huynh, FAW JKU
 *
 */

package core.prepr;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.zip.DataFormatException;

public abstract class DataReader {
	public static enum DATA_FORMATS {CSV, ARFF};
	
	protected DATA_FORMATS data_format;
	
	protected String delimiter = ",";
	
	protected BufferedReader input;
	
	/**
	 * List of attributes
	 */
	protected List<Attribute> attributes = new ArrayList<Attribute>();
	
	/**
	 * List of selectors
	 */
	protected List<Selector> selectors;
	
	/**
	 * List of constructing selectors includes:
	 * </br>+ FREQUENT selectors from predictive attributes in ascending order of support count
	 * </br>+ All selectors from target attributes in ascending order of support count
	 */
	protected List<Selector> constructing_selectors;
	
	/**
	 * Some scalar values
	 */
	protected int row_count = 0, min_sup_count;
	protected int attr_count, predict_attr_count, target_attr_count, numeric_attr_count=0;
	protected int distinct_value_count;
	protected int constructing_selector_count, predict_constructing_selector_count, target_selector_count;
	
	public DATA_FORMATS getDataFormat(){
		return this.data_format;
	}
	
	public List<Attribute> getAttributes() {
		return this.attributes;
	}

	public List<Selector> getSelectors() {
		return this.selectors;
	}

	public List<Selector> getConstructingSelectors() {
		return this.constructing_selectors;
	}

	public int getRowCount() {
		return this.row_count;
	}

	public int getMinSupCount() {
		return min_sup_count;
	}

	public int getAttrCount() {
		return attr_count;
	}

	public int getPredictAttrCount() {
		return predict_attr_count;
	}

	public int getTargetAttrCount() {
		return target_attr_count;
	}

	public int getNumericAttrCount() {
		return numeric_attr_count;
	}

	public int getDistinctValueCount() {
		return distinct_value_count;
	}

	public int getConstructingSelectorCount() {
		return constructing_selector_count;
	}

	public int getPredictConstructingSelectorCount() {
		return predict_constructing_selector_count;
	}

	public int getTargetSelectorCount() {
		return target_selector_count;
	}
	
	/**
	 * Fetch information from an input dataset
	 * @param datasource_filename
	 * @param target_attr_count
	 * @param support_threshold
	 * @throws DataFormatException, IOException
	 */
	public abstract void fetch_info(String datasource_filename,
									int target_attr_count,
									double support_threshold) throws DataFormatException, IOException;
	
	/**
	 * Open a data source file. Then using 'next_record()' method to fetch data row in form of String[]
	 * @param data_filename
	 * @throws DataFormatException, IOException
	 */
	public abstract void bind_datasource(String data_filename) throws DataFormatException, IOException;
	
	/**
	 * Support two formats: .csv, .arff
	 * @param file_name
	 * @return the corresponding CSVReader or ARFFReader, <b>null</b> if unsupported format
	 */
	public static final DataReader getDataReader(String file_name){
		DATA_FORMATS df = getDataFormat(file_name);
        
		switch(df){
			case ARFF:
				return new ARFFReader();
			case CSV:
				return new CSVReader();
			default:
	    		return null;
		}
	}
	
	protected static DATA_FORMATS getDataFormat(String file_name){
		String ext_name = file_name.substring(file_name.lastIndexOf('.')+1).toUpperCase();
		DATA_FORMATS format = DATA_FORMATS.valueOf(ext_name);
		return format;
	}
	
	/**
	 * Set a new delimiter to parse data section.
	 * </br>Default value is ','
	 * @param delimiter
	 */
	public void set_delimiter(String delimiter){
		this.delimiter = delimiter;
	}
	
	protected void prepare_selectors(){
		/**
	     * 1. Construct the selector list 'constructing_selectors' which includes:
	     * 	+ FREQUENT selectors from predictive attributes in ASCENDING ORDER of support count
	     *  + All selectors from target attributes which are in ASCENDING ORDER of support count
	     */
	    
    	/**
    	 * 1.1. Construct list 'predict_selectors' of selectors from the PREDICTIVE attributes
    	 *      in ASCENDING ORDER of support count
    	 */
	    List<Selector> predict_selectors = new ArrayList<Selector>();
		for(int i=0; i<this.predict_attr_count; i++){
			// The target attribute(s) at the end of the attribute list
			for(Selector sel: this.attributes.get(i).distinct_values.values()){
				predict_selectors.add(sel);
			}
		}
		Collections.sort(predict_selectors, new IncreaseFreqComparator());
	    
	    /**
	     * 1.2. Construct list 'target_selectors' of selectors from the TARGET attributes
	     *      in ASCENDING ORDER of support count
	     */
		List<Selector> target_selectors = new ArrayList<Selector>();
		for(int i=this.predict_attr_count; i<this.attr_count; i++){
			// The target attribute(s) at the end of the attribute list
			for(Selector sel: this.attributes.get(i).distinct_values.values()){
				target_selectors.add(sel);
			}
		}
		Collections.sort(target_selectors, new IncreaseFreqComparator());
		
		/**
		 * 1.3. 'constructing_selectors' = FREQUENT selectors in 'predict_selectors' appends all selectors in 'target_selectors'
		 * 1.4. 'selectors' = 'predict_selectors' appends 'target_selectors'
		 */
		this.selectors = new ArrayList<Selector>(predict_selectors.size()+target_selectors.size());
		this.constructing_selectors = new ArrayList<Selector>(predict_selectors.size()+target_selectors.size());
		
		for(Selector sel : predict_selectors){
			this.selectors.add(sel);
			if(sel.frequency < min_sup_count) continue;
			this.constructing_selectors.add(sel);
		}
		this.predict_constructing_selector_count = this.constructing_selectors.size();
		
		for(Selector sel : target_selectors){
			this.selectors.add(sel);
			this.constructing_selectors.add(sel);
		}
		
		this.distinct_value_count = this.selectors.size();
		this.constructing_selector_count = this.constructing_selectors.size();
		this.target_selector_count = this.constructing_selector_count - this.predict_constructing_selector_count;
		
		/**
		 * 2. Assign IDs
		 */
		// Assign the index in 'constructing_selectors' list as the selector ID
		// Infrequent selectors of predictive attributes will remain INVALID_ID
		for(int i=0; i<constructing_selector_count; i++) this.constructing_selectors.get(i).selectorID = i;
				
		// Assign the index in 'selectors' list as distinctValueID
		for(int i=0; i<distinct_value_count; i++) this.selectors.get(i).distinctValueID = i;
	}
	
	
	/**
	 * NOTE: This function is just used just for MAKING DEMONSTRATION
	 * <br>Because this function does not differentiate target selectors from predictive selectors
	 * <br>Predictive and Target selectors in the same group
	 */
	protected void prepare_selectors_PredictTargetSelectors_in_one_group(){		
		/**
	     * 1. Construct the selector list 'constructing_selectors' which includes:
	     * 	+ FREQUENT selectors from predictive attributes in ASCENDING ORDER of support count
	     *  + All selectors from target attributes which are in ASCENDING ORDER of support count
	     */
	    
    	/**
    	 * 1.1. Construct list 'predict_selectors' of selectors from the PREDICTIVE attributes
    	 */
	    List<Selector> predict_selectors = new ArrayList<Selector>();
		for(int i=0; i<this.predict_attr_count; i++){
			// The target attribute(s) at the end of the attribute list
			for(Selector sel: this.attributes.get(i).distinct_values.values()){
				predict_selectors.add(sel);
			}
		}
	    
	    /**
	     * 1.2. Construct list 'target_selectors' of selectors from the TARGET attributes
	     */
		List<Selector> target_selectors = new ArrayList<Selector>();
		for(int i=this.predict_attr_count; i<this.attr_count; i++){
			// The target attribute(s) at the end of the attribute list
			for(Selector sel: this.attributes.get(i).distinct_values.values()){
				target_selectors.add(sel);
			}
		}
		
		/**
		 * 1.3. 'constructing_selectors' = FREQUENT selectors in 'predict_selectors' and all selectors in 'target_selectors'
		 * 1.4. 'selectors' = 'predict_selectors' and 'target_selectors'
		 */
		this.selectors = new ArrayList<Selector>(predict_selectors.size()+target_selectors.size());
		this.constructing_selectors = new ArrayList<Selector>(predict_selectors.size()+target_selectors.size());
		
		for(Selector sel : predict_selectors){
			this.selectors.add(sel);
			if(sel.frequency < min_sup_count) continue;
			this.constructing_selectors.add(sel);
		}
		this.predict_constructing_selector_count = this.constructing_selectors.size();
		
		for(Selector sel : target_selectors){
			this.selectors.add(sel);
			this.constructing_selectors.add(sel);
		}
		
		this.distinct_value_count = this.selectors.size();
		this.constructing_selector_count = this.constructing_selectors.size();
		this.target_selector_count = this.constructing_selector_count - this.predict_constructing_selector_count;
		
		/**
		 * 2. Assign IDs
		 */		
		// Assign the index in 'constructing_selectors' list as the selector ID
		// Infrequent selectors of predictive attributes will remain INVALID_ID
		Collections.sort(this.constructing_selectors, new IncreaseFreqComparator());
		for(int i=0; i<constructing_selector_count; i++) this.constructing_selectors.get(i).selectorID = i;
				
		// Assign the index in 'selectors' list as distinctValueID
		Collections.sort(this.selectors, new IncreaseFreqComparator());
		for(int i=0; i<distinct_value_count; i++) this.selectors.get(i).distinctValueID = i;
	}
	
	
	
	/**
	 * Sequentially get the next record from the binded data source.
	 * </br>Require the data source that have already been binded with function <b>bind_datasource</b>
	 * @return string array of values
	 * @throws IOException 
	 */
	public String[] next_record() throws IOException{
		String line = null;
		
		if((line = input.readLine()) != null){
			return line.split(delimiter);
		}else{
			input.close();
			return null;
		}
	}
	
	/**
	 * Convert an example/instance (record of string values) to an array of the corresponding selector IDs
	 * @param instance an array of strings, read from the input dataset
	 * @param id_buffer input buffer, length is bigger than or equal the number of attributes in the input dataset
	 * @return The converted instance
	 */
	public int[] convert_instance(String[] instance, int[] id_buffer){
		int count=0;
		Selector s;
		
		for(int i=0; i<instance.length; i++){
			s = this.attributes.get(i).getSelector(instance[i]);
			if(s != null && s.selectorID != Selector.INVALID_ID){
				id_buffer[count] = s.selectorID;
				count++;
			}
		}
		
		int[] id_record = new int[count];
		System.arraycopy(id_buffer, 0, id_record, 0, count);
		
		return id_record;
	}
}
