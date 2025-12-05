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
 * DiffsetInfoBase is a class for holding information about a dataset and 
 * the Diffset structure build from the dataset. 
 * A basic diffset is a diffset of an item X (d(X) = T \ t(X)). 
 * T is the set of all transaction ids of the data source. 
 * t(X) is the tidset of item X. 
 */
public class DiffsetInfoBase {

	///////////////////////////////////////////////PROPERTIES//////////////////////////////////////////////////////
	
	// DiffsetInfoBase 成員
	protected core.prepr.DataReader cachedReader;

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
	
	protected IntegerArray[] basic_diffsets;
	
	
	///////////////////////////////////////////////GET/SET METHODS//////////////////////////////////////////////
	/**
     * Constructor
     */
    public DiffsetInfoBase(){}
    
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
    
    public void free_basic_diffsets(){
    	for(int i=0; i<this.basic_diffsets.length; i++){
    		this.basic_diffsets[i] = null;
    	}
    	this.basic_diffsets = null;
    }
    
    public IntegerArray[] getBasicDiffsets(){
    	return this.basic_diffsets;
    }
	
    
    ///////////////////////////////////////////////FUNCTIONALITY METHODS//////////////////////////////////////////////
    
    
    public long[] fetch_information(String file_name) throws IOException, DataFormatException {    	
    	long[] times = new long[2];
    	
        this.data_filename = file_name;
        
        times[0] = this.preprocessing();

        times[1] = this.build_diffsets();
        
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
    	
    	 core.prepr.DataReader dr = core.prepr.DataReader.getDataReader(this.data_filename);
    	if(dr == null){
    		System.out.println("Can not recognize the file type.");
    		return 0;
    	}
    	
    	dr.fetch_info(this.data_filename, this.target_attr_count, 0.0001);
		
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
		this.cachedReader = dr;
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
	
	/**
	 * Build the basic diffsets
	 * @return
	 * @throws IOException
	 * @throws DataFormatException
	 */
	protected long build_diffsets() throws IOException, DataFormatException {
	    long start = System.currentTimeMillis();
	    int emptyCount = 0, nonEmptyCount = 0;

	    // 每個 selector 對應一個 diffset
	    this.basic_diffsets = new IntegerArray[this.constructing_selector_count];
	    for (int i = 0; i < this.basic_diffsets.length; i++) {
	        this.basic_diffsets[i] = new IntegerArray();
	    }

	    int[][] result = new int[this.row_count][];
	    int index = 0;

	    core.prepr.DataReader dr = (this.cachedReader != null)
	    	    ? this.cachedReader
	    	    : core.prepr.DataReader.getDataReader(this.data_filename);


	    dr.bind_datasource(this.data_filename);

	    String[] value_record;
	    int[] id_buffer = new int[this.attr_count];

	    while ((value_record = dr.next_record()) != null) {
	        int[] id_record;

	        // 稀疏：單一 token 且以 '{' 開頭
	        if (value_record.length == 1 && value_record[0] != null
	                && value_record[0].trim().startsWith("{")) {
	            id_record = convert_sparse_line(value_record[0], id_buffer);
	        } else {
	            // 稠密
	            id_record = convert_dense_instance(value_record, id_buffer);
	        }

	        if (id_record.length == 0) {
	            emptyCount++;
	            continue; // 不寫入 result、不遞增 index
	        } else {
	            nonEmptyCount++;
	        }

	        java.util.Arrays.sort(id_record);
	        result[index] = id_record;
	        update_basic_diffsets(id_record, this.basic_diffsets, index);
	        index++;
	    }

	    System.out.println("[INFO] non-empty rows = " + nonEmptyCount + ", empty rows skipped = " + emptyCount);

	    // 裁切到實際有效列數 index
	    int[][] resized = new int[index][];
	    System.arraycopy(result, 0, resized, 0, index);
	    this.selectorID_records = resized;

	    // 將 row_count 改成有效非空筆數
	    this.row_count = index;

	    if (this.row_count == 0) {
	        System.out.println("[WARN] No non-empty transactions after preprocessing.");
	    }

	    // 省記憶體
	    for (IntegerArray diffset : this.basic_diffsets) diffset.shrink();

	    return System.currentTimeMillis() - start;
	}

	
	protected void update_basic_diffsets(int[] id_record, IntegerArray[] basic_diffsets, int tid){
		int i=0;
		int rec_sid = id_record[i];
		
		int sid = 0;
		for(; sid < basic_diffsets.length; sid++){
			if(sid < rec_sid){
				basic_diffsets[sid].add(tid);
			}else if(sid == rec_sid){
				i++;
				if(i < id_record.length){
					rec_sid = id_record[i];
				}
			}else{
				// sid > rec_sid
				// id_record is now consumed completely
				break;
			}
		}
		
		for(; sid < basic_diffsets.length; sid++){
			basic_diffsets[sid].add(tid);
		}
	}
	
	
	/**
	 * Convert an example/instance (record of string values) to an array of the corresponding selector IDs
	 * @param instance an array of strings, read from the input dataset
	 * @param id_buffer input buffer, length is bigger than or equal the number of attributes in the input dataset
	 * @return The converted instance
	 */
	private int[] convert_dense_instance(String[] tokens, int[] id_buffer) {
	    int count = 0;
	    int dimAttr = this.attributes.size();

	    // Reader 應已對齊欄位數：不足補 "?"、超出截斷
	    for (int i = 0; i < dimAttr; i++) {
	        String val = (i < tokens.length && tokens[i] != null) ? tokens[i].trim() : "?";
	        // Dense：直接用原值，不要預設 "0"
	        Selector s = this.attributes.get(i).getSelector(val);
	        if (s != null && s.selectorID != Selector.INVALID_ID) {
	            id_buffer[count++] = s.selectorID;
	        }
	    }
	    int[] id_record = new int[count];
	    System.arraycopy(id_buffer, 0, id_record, 0, count);
	    return id_record;
	}

	private int[] convert_sparse_line(String raw, int[] id_buffer) {
	    // raw 形如 "{1833 1, 2205 1, ...}"
	    java.util.Map<Integer, String> m = new java.util.HashMap<>();
	    String inside = raw.trim();
	    if (inside.startsWith("{") && inside.endsWith("}")) {
	        inside = inside.substring(1, inside.length() - 1).trim();
	    }
	    if (!inside.isEmpty()) {
	        for (String p : inside.split(",")) {
	            String[] iv = p.trim().split("\\s+");
	            if (iv.length >= 2) {
	                try {
	                    int idx = Integer.parseInt(iv[0]);
	                    m.put(idx, iv[1].trim());
	                } catch (NumberFormatException ignore) {}
	            }
	        }
	    }

	    // 稀疏：未列出的欄位預設為 "0"
	    int count = 0;
	    int dimAttr = this.attributes.size();
	    for (int i = 0; i < dimAttr; i++) {
	        String val = m.getOrDefault(i, "0");
	        Selector s = this.attributes.get(i).getSelector(val);
	        if (s != null && s.selectorID != Selector.INVALID_ID) {
	            id_buffer[count++] = s.selectorID;
	        }
	    }
	    int[] id_record = new int[count];
	    System.arraycopy(id_buffer, 0, id_record, 0, count);
	    return id_record;
	}


	
	/**
	 * Write the diffsets of selectors to a file. This function is used for test only.
	 * @param file_name
	 * @throws IOException
	 */
	public void export_basic_diffsets(String file_name) throws IOException{
    	BufferedWriter w = new BufferedWriter(new FileWriter(file_name));
    	StringBuffer sb = new StringBuffer(1024);
    	
    	int id = 0;
    	w.write("------Diffsets------\n");
    	for(IntegerArray diffset : this.basic_diffsets){
    		sb.setLength(0);
    		sb.append(id).append(": not(")
    		.append(this.constructing_selectors.get(id).condition)
    		.append(") -> ");
    		for(int i=0; i<diffset.size(); i++){
    			sb.append(diffset.get(i)).append(' ');
    		}
    		sb.append('\n');
    		w.write(sb.toString());
    		id++;
    	}
    	w.flush();
    	w.close();
    }
	
	/**
	 * Calculate set_1 union set_1
	 * @param set_1
	 * @param set_2
	 * @return
	 */
	protected IntegerArray generate_union_set(IntegerArray set_1, IntegerArray set_2){
		int size_1 = set_1.size();
		int size_2 = set_2.size();
		if (size_1 == 0){
			return set_2.copy();
		}
		if (size_2 == 0){
			return set_1.copy();
		}
		IntegerArray union_set = new IntegerArray();
		int i1 = 0, i2 = 0;
		int tid1 = set_1.get(0);
		int tid2 = set_2.get(0);
		
		while (true){
			if (tid1 == tid2){
				union_set.add(tid1);
				i1++;
				i2++;
				if(i1 < size_1)	tid1 = set_1.get(i1);	// may be out of index after increase i1
				else{
					// add the remaining tids in diffset_2 to result_diffset
					for(int i=i2; i<size_2; i++){
						union_set.add(set_2.get(i));
					}
					break;
				}
				if(i2 < size_2) tid2 = set_2.get(i2);  // may be out of index after increase i2
				else{
					// add the remaining tids in diffset_1 to result_diffset
					for(int i=i1; i<size_1; i++){
						union_set.add(set_1.get(i));
					}
					break;
				}
			}else if(tid1 < tid2){
				union_set.add(tid1);
				i1++;
				if(i1 < size_1)	tid1 = set_1.get(i1);
				else{
					// add the remaining tids in diffset_2 to result_diffset
					for(int i=i2; i<size_2; i++){
						union_set.add(set_2.get(i));
					}
					break;
				}
			}else{
				union_set.add(tid2);
				i2++;
				if(i2 < size_2) tid2 = set_2.get(i2);
				else{
					// add the remaining tids in diffset_1 to result_diffset
					for(int i=i1; i<size_1; i++){
						union_set.add(set_1.get(i));
					}
					break;
				}
			}
		}
		
		return union_set;
	}
	
	public IntegerArray calculate_union_set(int[] itemset){
		IntegerArray union_set = this.basic_diffsets[itemset[0]];
		
		for(int i = 1; i < itemset.length; i++){
			union_set = this.generate_union_set(union_set, this.basic_diffsets[itemset[i]]);
		}
		return union_set;
	}
	
	/**
	 * Calculate set_1 - set_2 
	 * @param set_1
	 * @param set_2
	 * @return
	 */
	protected IntegerArray generate_different_set(IntegerArray set_1, IntegerArray set_2){
		int size_1 = set_1.size();
		int size_2 = set_2.size();
		if (size_1 == 0){
			return new IntegerArray();
		}
		if (size_2 == 0){
			return set_1.copy();
		}
		
		IntegerArray diffset = new IntegerArray();
		int i1 = 0, i2 = 0;
		int tid1 = set_1.get(i1);
		int tid2 = set_2.get(i2);
		
		while (true){
			if(tid1 == tid2){
				i1++;
				i2++;
				if(i1 < size_1) tid1 = set_1.get(i1);
				else break;
				
				if(i2 < size_2) tid2 = set_2.get(i2);
				else{
					for(int i=i1; i<size_1; i++) diffset.add(set_1.get(i));
					break;
				}
			}else if(tid1 < tid2){
				diffset.add(tid1); 
				i1++;
				if(i1 < size_1) tid1 = set_1.get(i1);
				else break;
			}else{
				i2++;
				if(i2 < size_2) tid2 = set_2.get(i2);
				else{
					for(int i=i1; i<size_1; i++) diffset.add(set_1.get(i));
					break;
				}
			}
		}

		return diffset;
	}
	
	/**
	 * This implements an optimized calculation for support count of an itemset based on diffset
	 * Example: itemset = 1234, (d(1234) is diffset of itemset 1234), T is the set of all transaction ids
	 * <br> sup(1234) = sup(123) - |d(1234)|
	 * <br> sup(123) = sup(12) - |d(123)|
	 * <br> sup(12) = sup(1) - |d(12)|
	 * <br> sup(1) = |T| - |d(1)|
	 * <br> ==> sup(1234) = |T| - |d(1)| - |d(12)| - |d(123)| - |d(1234)|
	 * <br> 
	 * <br> d(1234)				 = d(124) - d(123)
	 * <br> d(123)  d(124)
	 * <br> d(12)	d(13)	d(14)
	 * <br> d(1)	d(2)	d(3)	d(4) 
	 * @param itemset
	 * @return
	 */
	public int calculate_supportcount_for_itemset(int[] itemset){
		int support_count = this.row_count;
		
		List<IntegerArray> layer = new ArrayList<IntegerArray>(itemset.length);
		for(int item : itemset){
			layer.add(this.basic_diffsets[item]);
		}
		
		while(layer.size() > 0){
			IntegerArray first_diffset = layer.get(0);
			support_count -= first_diffset.size();
			
			List<IntegerArray> next_layer = new ArrayList<IntegerArray>(layer.size()-1);
			for(int i=1; i<layer.size(); i++){
				next_layer.add(this.generate_different_set(layer.get(i), first_diffset));
			}
			layer = next_layer;
		}
		
		return support_count;
	}
//	public int calculate_supportcount_for_itemset(int[] itemset){
//		int support_count = this.row_count;
//		
//		IntegerArray[] layer = new IntegerArray[itemset.length];
//		int i=0;
//		for(int item : itemset){
//			layer[i] = this.basic_diffsets[item];
//			i++;
//		}
//		
//		while(layer.length > 0){
//			IntegerArray first_diffset = layer[0];
//			support_count -= first_diffset.size();
//			
//			IntegerArray[] next_layer = new IntegerArray[layer.length-1];
//			i=0;
//			for(int j=1; j<layer.length; j++){
//				next_layer[i] = this.generate_different_set(layer[j], first_diffset);
//				i++;
//			}
//			layer = next_layer;
//		}
//		
//		return support_count;
//	}
	
	
	///////////////////////////////////////////////BENCHMARK METHODS//////////////////////////////////////////////
}
