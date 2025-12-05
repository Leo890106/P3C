/*
 * @author Van Quoc Phuong Huynh, FAW JKU
 *
 */

package tidsetbase;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.DataFormatException;

import core.prepr.Attribute;
import core.prepr.DataReader;
import core.prepr.Selector;

/**
 * TidsetInfoBase is a class for holding information about a feeding dataset and 
 * the Tidset structure (Eclat algorithm) build from the dataset.
 */
public class TidsetInfoBase {

	///////////////////////////////////////////////PROPERTIES//////////////////////////////////////////////////////
	
	/**
	 * The number of threads to run in some processing, i.e. discretize numeric attributes
	 */
    protected int thread_count = Math.max(2, Runtime.getRuntime().availableProcessors()/2);
    
    /**
     * Data file name
     */
    protected String data_filename = null;
	
    /**
     * The number of records in the input dataset
     */
    protected int row_count;
    
    /**
     * A minimum support count, applied on selectors of predictive attributes, but not on selectors of the target attribute(s)
     */
    protected int min_sup_count;
    
    /**
     * The number of attributes in the dataset
     */
    protected int attr_count;

    /**
     * The number of predictive attributes in the dataset
     */
    protected int predict_attr_count;
    
    /**
     * The number of target attributes in the dataset, the last attribute(s) in the attributes list
     */
    protected int target_attr_count = 1;

    /**
     * The number of numeric attributes in the dataset
     */
	protected int numeric_attr_count;
	
	/**
	 * The number of distinct values/features in the dataset
	 */
	protected int distinct_value_count;
	
	/**
	 * The number of constructing selectors in the dataset
	 */
	protected int constructing_selector_count;
	
	/**
	 * The number of frequent selectors from the predictive attributes
	 */
	protected int predict_constructing_selector_count;
	
	/**
	 * The number of all selectors from the target attribute(s), is also the number of classes
	 */
	protected int target_selector_count;
	
	/**
	 * List of class IDs which are the selector IDs from the target attribute(s)
	 */
	protected List<Integer> classIDs;
	
	/**
	 * List of attributes in the dataset, attribute ID = its index in the attribute list
	 */
	protected List<Attribute> attributes;
	
	/**
	 * <b>List of constructing selectors</b> including two groups (in sequence):
	 * </br>1. FREQUENT selectors (features) from predictive attributes
	 * </br>2. All selectors (features) from the target attribute(s)
	 * </br> Note: All selectors in this list are in ascending order of their frequencies, 
	 * and selector ID = its index in the list, so a selector with larger ID is more frequent
	 */
	protected List<Selector> constructing_selectors;
	
	/**
	 * Instances/examples in the input dataset encoded in arrays of sorted selector IDs.
	 * </br>Note that: a selector with larger ID covers more examples (more frequent)
	 */
	protected int[][] selectorID_records;
	
	protected IntegerArray[] basic_tidsets;
	
	
	///////////////////////////////////////////////GET/SET METHODS//////////////////////////////////////////////
	/**
     * Constructor
     */
    public TidsetInfoBase(){}
    
    /**
     * Set a desired number of threads for some processing, i.e. discretize numeric attributes
     * @param thread_num number of threads to run
     * @param can_exceed_core_num whether the desired number of threads can exceed the number of physical cores.
     */
    public void setThreadCount(int thread_num, boolean can_exceed_core_num){
    	if(can_exceed_core_num){
    		if(thread_num > 0) this.thread_count = thread_num;
    	}else{
    		if(thread_num > 0) this.thread_count = Math.min(this.thread_count, thread_num);
    	}
    }
    
    public int getThreadCount(){
    	return this.thread_count;
    }
    
    /**
     * @return Input data file name
     */
    public String getDataFilename(){
    	return this.data_filename;
    }
    
    /**
     * @return The number of examples/instances in the input dataset
     */
    public int getRowCount(){
    	return this.row_count;
    }
    
    /**
     * @return The minimum support count 
     */
    public double getMinSupCount(){
    	return this.min_sup_count;
    }
    
    /**
     * @return The number of attributes in the input dataset
     */
    public int getAttrCount(){
    	return this.attr_count;
    }
    
    /**
     * @return The number of predictive attributes
     */
    public int getPredictAttrCount(){
    	return this.predict_attr_count;
    }
    
    /**
     * @return The number of target attributes
     */
    public int getTargetAttrCount() {
		return target_attr_count;
	}

    /**
     * @return The number of numeric attributes
     */
	public int getNumericAttrCount() {
		return numeric_attr_count;
	}

	/**
	 * @return The number of distinct values from all attributes in the input dataset
	 */
	public int getDistinctValueCount() {
		return distinct_value_count;
	}
	
	/**
	 * @return The number of constructing selectors from the input dataset
	 */
	public int getConstructingSelectorCount(){
		return this.constructing_selector_count;
	}
	
	/**
	 * @return The number frequent selectors from predictive attributes
	 */
	public int getPredictConstructingSelectorCount(){
		return this.predict_constructing_selector_count;
	}
	
	/**
	 * @return The number all selectors from the target attribute(s), is also the number of classes
	 */
	public int getTargetSelectorCount(){
		return this.target_selector_count;
	}
	
	/**
     * @return The list of class IDs (selector IDs from the target attribute(s)) 
     */
    public List<Integer> getClassIDs(){
    	return this.classIDs;
    }
    
	/**
	 * @return List of attributes in the input dataset
	 */
	public List<Attribute> getAttributes(){
		return this.attributes;
	}
	
	/**
	 * <b>List of constructing selectors</b> including two groups (in sequence):
	 * </br>1. FREQUENT selectors (features) from predictive attributes
	 * </br>2. All selectors (features) from the target attribute(s)
	 * </br> Note: All selectors in this list are in ascending order of their frequencies, 
	 * and selector ID = its index in the list, so a selector with larger ID is more frequent
	 */
	public List<Selector> getConstructingSelectors(){
		return this.constructing_selectors;
	}
    
    /**
     * @return Instances/examples (in the input dataset) encoded in arrays of sorted selector IDs
     */
    public int[][] getSelectorIDRecords(){
    	return this.selectorID_records;
    }
    
    public IntegerArray[] getBasicTidsets(){
    	return this.basic_tidsets;
    }
	
    
    ///////////////////////////////////////////////FUNCTIONALITY METHODS//////////////////////////////////////////////
    
    
    public long[] fetch_information(String file_name) throws IOException, DataFormatException {    	
    	long[] times = new long[2];
    	
        this.data_filename = file_name;
        
        times[0] = this.preprocessing();
        
        times[1] = this.build_tidsets();
        
        return times;
    }
  
    
    /**
     * Read the input dataset to extract information about attributes, distinct values, selectors, etc.
     * @return running time
     * @throws IOException
     * @throws DataFormatException 
     */
	protected long preprocessing() throws IOException, DataFormatException {
    	long start = System.currentTimeMillis();
    	
    	DataReader dr = DataReader.getDataReader(this.data_filename);
    	if(dr == null){
    		System.out.println("Can not recognize the file type.");
    		return 0;
    	}
    	
    	dr.fetch_info(this.data_filename, this.target_attr_count, 0.001);
		
		this.attributes = dr.getAttributes();
		
		this.constructing_selectors = dr.getConstructingSelectors();
		
		this.row_count = dr.getRowCount();
		this.min_sup_count = dr.getMinSupCount();
		
		this.attr_count = dr.getAttrCount();
		this.predict_attr_count = dr.getPredictAttrCount();
		this.target_attr_count = dr.getTargetAttrCount();
		this.numeric_attr_count = dr.getNumericAttrCount();
		this.distinct_value_count = dr.getDistinctValueCount();
		
		this.constructing_selector_count = dr.getConstructingSelectorCount();
		this.predict_constructing_selector_count = dr.getPredictConstructingSelectorCount();
		this.target_selector_count = dr.getTargetSelectorCount();
		
		this.classIDs = this.get_class_ids();	// all class IDs
		
        return System.currentTimeMillis() - start;
    }
	protected List<Integer> get_class_ids(){
    	List<Integer> classIDs = new ArrayList<Integer>(this.target_selector_count);
    	
    	Attribute target_attr = this.attributes.get(this.attr_count-1);
    	for (Selector s : target_attr.distinct_values.values()){
    		classIDs.add(s.selectorID);
    	}
    	
    	return classIDs;
    }
	
	protected long build_tidsets() throws IOException, DataFormatException {
		long start = System.currentTimeMillis();  
		
		// each selector/item associates with a tidset
		this.basic_tidsets = new IntegerArray[this.constructing_selector_count];
		for(int i=0; i<this.basic_tidsets.length; i++){
			this.basic_tidsets[i] = new IntegerArray();
		}
		
		int[][] result = new int[this.row_count][];
		int index = 0;
	    
		DataReader dr = DataReader.getDataReader(this.data_filename);
		dr.bind_datasource(this.data_filename);
		
		String[] value_record;
		int[] id_buffer = new int[this.attr_count];
		int[] id_record;
		
		while((value_record = dr.next_record()) != null){
			// convert value_record to a record of selectorIDs
			result[index] = id_record = this.convert_instance(value_record, id_buffer);
			
			// selectors with higher frequencies have greater selector ID
			// only support ascending sort, so the order of ids to insert to the tree is from right to left
			// since id of a target selector is always greater than id of predictive selector
			// sorting id_record will NOT blend the IDs of two kinds of selectors together
			Arrays.sort(id_record);
			
			// add transaction id to the corresponding tidsets
			for(int selector_id : id_record){
				this.basic_tidsets[selector_id].add(index);
			}
			index++;
		}
		
		// Shrink the basic tidsets to save memory
		for(IntegerArray tidset: this.basic_tidsets){
			tidset.shrink();
		}
		
		this.selectorID_records = result;
		
	    return System.currentTimeMillis() - start;
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
	
	/**
	 * Write the tidsets of selectors to a file. This function is used for test only.
	 * @param file_name
	 * @throws IOException
	 */
	public void export_basic_tidsets(String file_name) throws IOException{
    	BufferedWriter w = new BufferedWriter(new FileWriter(file_name));
    	StringBuffer sb = new StringBuffer(1024);
    	
    	int id = 0;
    	w.write("------Tidsets------\n");
    	for(IntegerArray tidset : this.basic_tidsets){
    		sb.setLength(0);
    		sb.append(id).append(": ")
    		.append(this.constructing_selectors.get(id).condition)
    		.append(" -> ");
    		for(int i=0; i<tidset.size(); i++){
    			sb.append(tidset.get(i)).append(' ');
    		}
    		sb.append('\n');
    		w.write(sb.toString());
    		id++;
    	}
    	w.flush();
    	w.close();
    }
	
	protected IntegerArray create_tidset(IntegerArray tidset_1, IntegerArray tidset_2){
		IntegerArray result_tidset = new IntegerArray();
		int size_1 = tidset_1.size();
		int size_2 = tidset_2.size();
		if (size_1 == 0 || size_2 == 0){
			return result_tidset;
		}
		int i1 = 0, i2 = 0;
		int tid1 = tidset_1.get(0);
		int tid2 = tidset_2.get(0);
		
		while (true){
			if (tid1 == tid2){
				result_tidset.add(tid1);
				i1++;
				if(i1 < size_1)	tid1 = tidset_1.get(i1);	// may be out of index after increase i1
				else break;
				i2++;
				if(i2 < size_2) tid2 = tidset_2.get(i2);
				else break;
			}else if(tid1 < tid2){
				i1++;
				if(i1 < size_1)	tid1 = tidset_1.get(i1);
				else break;
			}else{
				i2++;
				if(i2 < size_2) tid2 = tidset_2.get(i2);
				else break;
			}
		}
		return result_tidset;
	}
	
	public IntegerArray create_tidset_for_itemset(int[] itemset){
		IntegerArray tidset = this.basic_tidsets[itemset[0]];
		
		for(int i = 1; i < itemset.length; i++){
			tidset = this.create_tidset(tidset, this.basic_tidsets[itemset[i]]);
		}
		return tidset;
	}
	
	
	///////////////////////////////////////////////BENCHMARK METHODS//////////////////////////////////////////////
}
