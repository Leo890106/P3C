/*
 * @author Van Quoc Phuong Huynh, FAW JKU
 *
 */

package nlistbase;

import java.io.BufferedWriter;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.zip.DataFormatException;

import core.prepr.Attribute;
import core.prepr.DataReader;
import core.prepr.Selector;
import core.structure.INlist;
import core.structure.PPCNode;
import core.structure.PPCTree;
import core.structure.P3CTree;
import core.structure.Supporter;
import core.structure.P3CNode;

/**
 * InfoBase is a class for holding information about a feeding dataset and the Nlist structure build from the dataset.
 * It provides methods to sum up efficiently the number of instances which support/satisfy conditions.
 * </br>Note: selectors, distinct values, features are concepts used interchangeably in this implementation.
 */
public class InfoBase {
	// DiffsetInfoBase 成員
		protected core.prepr.DataReader cachedReader;
		private double p3cTotMemoryMB;
		private double p3cTotAbsMemoryMB;

		public double getP3cTotMemoryMB() {
		    return p3cTotMemoryMB;
		}
		private double p3cTotMB;

	    public double getP3cTotMB() {
	        return p3cTotMB;
	    }
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
	 * Array of Nlists of selectors, One at i-th index in the array is Nlist of selector with ID i
	 */
	protected INlist[] selector_nlists;
	
	/**
	 * Nlists of selectors stored in a map, from a selector ID to the corresponding Nlist
	 */
	protected Map<String, INlist> selector_nlist_map;
	
	/**
	 * Instances/examples in the input dataset encoded in arrays of sorted selector IDs.
	 * </br>Note that: a selector with larger ID covers more examples (more frequent)
	 */
	protected int[][] selectorID_records;
	
	/**
	 * The expected memory efficiency coefficient
	 */
	protected long efficiency = 1000;
	
	/**
	 * 
	 */
	protected int furtherEfficiency = -1;
	
	
	///////////////////////////////////////////////GET/SET METHODS//////////////////////////////////////////////
	/**
     * Constructor
     */
    public InfoBase(){}
    
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
     * @return Array of Nlists of selectors, One at i-th index in the array is Nlist of selector with ID i
     */
    public INlist[] getSelectorNlists(){
    	return this.selector_nlists;
    }
    
    /**
     * @return Nlists of selectors stored in a map, from a selector ID to the corresponding Nlist
     */
    public Map<String, INlist> getSelectorNlistMap(){
    	return this.selector_nlist_map;
    }
    
    /**
     * @return Instances/examples (in the input dataset) encoded in arrays of sorted selector IDs
     */
    public int[][] getSelectorIDRecords(){
    	return this.selectorID_records;
    }
	
    /**
     * Set the expected memory efficiency coefficient, 
     * the maximum number of instances to build a subtree < total_instances/efficiency
     * @param value
     */
    public void setEfficiency(long value){
    	this.efficiency = value;
    }
    
    /**
     * Get the expected memory efficiency coefficient, 
     * the maximum number of instances to build a subtree < total_instances/efficiency
     * @return
     */
    public long getEfficiency(){
    	return this.efficiency;
    }
    
    /**
     * Get a recommended efficiency coefficient for a further efficiency
     * @return
     */
    public int getFurtherEfficiency(){
    	return this.furtherEfficiency;
    }
    
    ///////////////////////////////////////////////FUNCTIONALITY METHODS//////////////////////////////////////////////
    
    /**
     * Fetch information from the input dataset.
     * </br>1. Do data preprocessing
     * </br>2. Build the global PPCTree from the input dataset
     * </br>3. Create Nlist for each distinct selector
     * </br><b>Note that:</b> 
     * <ul>
     * <li> All attributes in the input .CSV file will be treated as categorical atrributes</li>
     * <li>Numeric attributes in the input .ARFF file will be discretized automatically</li>
     * </ul>
     * @param file_name The input dataset file name
     * @return running time of the three stages: [0] preprocessing, [1] build tree, [2] Nlist for each distinct selector
     * @throws IOException
     * @throws DataFormatException
     */
    public long[] fetch_information(String file_name) throws IOException, DataFormatException {
    	long[] times = new long[3];
    	
        this.data_filename = file_name;
        
        times[0] = this.preprocessing();
        
        PPCTree ppcTree = new PPCTree(); 
        times[1] = this.construct_tree(ppcTree);
        
        // Store the tree, just be used for testing
        //ppcTree.storeTree("data/output/ppc_tree_full");
        
        long start = System.currentTimeMillis();
        this.selector_nlists = ppcTree.create_Nlist_for_selectors_arr(this.constructing_selector_count);
        this.selector_nlist_map = ppcTree.create_selector_Nlist_map(this.selector_nlists);
        
        times[2] = System.currentTimeMillis() - start;
        
        return times;
    }
    
    /**
     * Fetch information from the input dataset. Use this method when the input data is big and
     * the available memory is not enough to render the global PPCTree built from dataset.
     * </br>1. Do data preprocessing
     * </br>2. Build the top part of the global PPCTree, the height of this top part is determined by 'level' property
     * </br>3. Build subtrees and update Nlists for each distinct selector
     * </br><b>Note that:</b> 
     * <ul>
     * <li> All attributes in the input .CSV file will be treated as categorical atrributes</li>
     * <li>Numeric attributes in the input .ARFF file will be discretized automatically</li>
     * </ul>
     * @param file_name The input dataset file name
     * @return running time of the three stages: [0] preprocessing, [1] build tree top part, [2] Build subtrees and update Nlist for each distinct selector
     * @throws IOException
     * @throws DataFormatException
     */
    public long[] fetch_information_with_memory_efficiency(String file_name) throws IOException, DataFormatException {
    	long[] times = new long[3];
    	
        this.data_filename = file_name;
        
        times[0] = this.preprocessing();
        
        // Build the top part of the global PPCtree
        P3CTree p3ctree = new P3CTree(this.constructing_selector_count); 
        times[1] = this.construct_tree_top_part(p3ctree);
        
        // Build subtrees and update Nlist for each selector
        long start = System.currentTimeMillis();
        
        List<PPCNode> leaf_nodes = p3ctree.getLeafNodes();
        
        for (PPCNode leaf_node : leaf_nodes){
        	// Build a subtree with root at leaf_node
        	p3ctree.buildSubtree(leaf_node);
        	
        	// Assign pre-order and post-order codes
        	p3ctree.assignPrePosOrderCodeSubTree(leaf_node);
        	
        	// Update Nlist of selectors and free the subtree
        	p3ctree.update_nlists_from_subtree(leaf_node);
        	
        	// Free the subtree with root at leaf_node for memory
        	p3ctree.freeSubTrees(leaf_node);
        }
        
        p3ctree.shrink_nlists();
        this.selector_nlists = p3ctree.get_selector_nlists();
        this.selector_nlist_map = p3ctree.create_selector_Nlist_map(this.selector_nlists);
    	
        times[2] = System.currentTimeMillis() - start;
        System.out.println("Preprocessing time: " + times[0] + " ms");
		System.out.println("Diffsets construction time: " + times[1] + " ms");
        
        // recommend a further efficiency coefficient based on 
        // the max number of instances used to build a subtree        
        int max_inst_count = 0;
        for(PPCNode node : leaf_nodes){
        	if (max_inst_count < node.count) {
        		max_inst_count = node.count;
        	}
        }
        
        this.furtherEfficiency = this.row_count/((max_inst_count )/2);
        
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
	 * Read the input dataset the second time to build a tree to construct N-list structures
	 * @return running time
	 * @throws IOException
	 * @throws DataFormatException 
	 */
	protected long construct_tree(PPCTree tree) throws IOException, DataFormatException {
		long start = System.currentTimeMillis();  
		
		int[][] result = new int[this.row_count][];
		int index = 0;
	    
		core.prepr.DataReader dr = (this.cachedReader != null)
	    	    ? this.cachedReader
	    	    : core.prepr.DataReader.getDataReader(this.data_filename);
		dr.bind_datasource(this.data_filename);
		dr.bind_datasource(this.data_filename);
		
		String[] value_record;
		
		int[] id_buffer = new int[this.attr_count];
		int[] id_record;
		
		while((value_record = dr.next_record()) != null){
			// convert value_record to a record of selectorIDs
			// 稀疏：單一 token 且以 '{' 開頭
	        if (value_record.length == 1 && value_record[0] != null
	                && value_record[0].trim().startsWith("{")) {
	            id_record = convert_sparse_line(value_record[0], id_buffer);
	        } else {
	            // 稠密
	            id_record = convert_dense_instance(value_record, id_buffer);
	        }
			result[index] = id_record;
			index++;
			
			// selectors with higher frequencies have greater selector ID
			// only support ascending sort, so the order of ids to insert to the tree is from right to left
			// since id of a target selector is always greater than id of predictive selector
			// sorting id_record will NOT blend the IDs of two kinds of selectors together	
			Arrays.sort(id_record);
			
			// System.out.println(Arrays.toString(id_record));	// for testing
			
			tree.insert_record(id_record);
		}

		this.selectorID_records = result;
	    
		// Assign a pair of pre-order and pos-order codes for each tree node.
		tree.assignPrePosOrderCode();
		
	    return System.currentTimeMillis() - start;
	}
	
	/**
	 * Read the input dataset to build the top part of the global tree
	 * @return running time
	 * @throws IOException
	 * @throws DataFormatException 
	 */
	protected long construct_tree_top_part(P3CTree tree) throws IOException, DataFormatException {
		long start = System.currentTimeMillis();
		
		int[][] data_instances = new int[this.row_count][];
		
		core.prepr.DataReader dr = (this.cachedReader != null)
	    	    ? this.cachedReader
	    	    : core.prepr.DataReader.getDataReader(this.data_filename);
		dr.bind_datasource(this.data_filename);
		
		String[] value_record;
		int[] id_buffer = new int[this.attr_count];
		int[] id_record;
		int index = 0;
		while ((value_record = dr.next_record()) != null) {
	        // 稀疏：單一 token 且以 '{' 開頭
	        if (value_record.length == 1 && value_record[0] != null
	                && value_record[0].trim().startsWith("{")) {
	            id_record = convert_sparse_line(value_record[0], id_buffer);
	        } else {
	            // 稠密
	            id_record = convert_dense_instance(value_record, id_buffer);
	        }

	        java.util.Arrays.sort(id_record);
	        data_instances[index] = id_record;
	        index++;
	    }
		this.selectorID_records = data_instances;
		
		// The max number of instances to build a sub tree with its root at a leaf node of the top part
		long max_inst_count = this.row_count/this.efficiency;

		tree.buildTopPart(data_instances, max_inst_count);
		
	    return System.currentTimeMillis() - start;
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
	 * Write the Nlist of selectors to a file
	 * @param file_name
	 * @throws IOException
	 */
	public void export_nlists(String file_name) throws IOException{
    	BufferedWriter w = new BufferedWriter(new FileWriter(file_name));
    	int id = 0;
    	for(INlist nlist : this.selector_nlists){
    		w.write(this.constructing_selectors.get(id).condition);
    		w.write(" -> ");
    		w.write(nlist.toString());
    		w.write('\n');
    		id++;
    	}
    	w.flush();
    	w.close();
    }
	
	public INlist create_nlist_for_itemset(int[] itemset){
		INlist nlist = this.selector_nlists[itemset[0]];
		
		for(int i = 1; i < itemset.length; i++){
			nlist = Supporter.create_nlist(nlist, this.selector_nlists[itemset[i]]);
		}
		return nlist;
	}
	
	
	///////////////////////////////////////////////BENCHMARK METHODS//////////////////////////////////////////////
	
	public long[] demonstrate_p3ctree(String file_name) throws IOException, DataFormatException {
    	long[] times = new long[3];
    	
        this.data_filename = file_name;
        
        times[0] = this.preprocessing();
        
        // Print list of selectors/items
        System.out.println("\nSelectors/Items list:");
        for(Selector s : this.constructing_selectors){
        	System.out.println(String.format("%d, %s, %s, %d", s.selectorID, s.attributeName, s.distinctValue, s.frequency));
        }
        
        // Build the top part of P3Ctree
        P3CTree p3ctree = new P3CTree(this.constructing_selector_count); 
        times[1] = this.construct_tree_top_part(p3ctree);
        
        // Print list of instances/transactions in the dataset
        System.out.println("\nInstances/Transaction list:");
        for(int[] instance : this.selectorID_records){
        	System.out.println(Arrays.toString(instance));
        }
        
        // Build subtrees and update Nlist for each selector
        long start = System.currentTimeMillis();
        
        System.out.println("\nTop part of P3Ctree:");
        
        List<PPCNode> leaf_nodes = p3ctree.getLeafNodes();
        for (PPCNode leaf_node : leaf_nodes){
        	
        	// print list of instances to build the sub tree
        	StringBuilder sb = new StringBuilder(200);
        	P3CNode top_ppc_node  = (P3CNode) leaf_node;
        	sb.append("\n\tlevel: ").append(top_ppc_node.instGroup.level);
        	for(int[] instance : top_ppc_node.instGroup.instances){
				sb.append("\n\t").append(Arrays.toString(instance));
			}
    		String instances = sb.toString();
    		
        	// Build a subtree with root at leaf_node
        	p3ctree.buildSubtree(leaf_node);
        	
        	// Assign pre-order and post-order codes
        	p3ctree.assignPrePosOrderCodeSubTree(leaf_node);
        	
        	// Update Nlist of selectors and free the subtree
        	p3ctree.update_nlists_from_subtree(leaf_node);
        	
        	sb = new StringBuilder(200);
        	PPCNode node = leaf_node;
    		while (node.parent != null){
    			sb.append(node.parent.pre).append(':')
    			.append(node.pre).append(':')
    			.append(node.pos).append(':')
    			.append(node.itemID).append(':')
    			.append(node.count).append(", ");
    			node = node.parent;
    		}
    		sb.append("Root");
    		sb.append(instances);
        	System.out.println(sb.toString());
        	
        	// Free the subtree with root at leaf_node for memory
        	p3ctree.freeSubTrees(leaf_node);
        }
        
        p3ctree.shrink_nlists();
        this.selector_nlists = p3ctree.get_selector_nlists();
        this.selector_nlist_map = p3ctree.create_selector_Nlist_map(this.selector_nlists);
    	
        times[2] = System.currentTimeMillis() - start;
        
        return times;
    }
	
	/**
	 * Benchmark consumed memory for PPCtree, encoded instances from data, Nlists of selectors
	 * @param file_name
	 * @throws IOException
	 * @throws DataFormatException
	 */
	public void benchmark_memory_for_ppctree(String file_name) throws IOException, DataFormatException {
        this.data_filename = file_name;
        long  tree_time = 0;
        this.preprocessing();
        long start = System.currentTimeMillis();
        PPCTree ppcTree = new PPCTree(); 
        this.construct_tree(ppcTree);
        tree_time +=  System.currentTimeMillis() - start;
        System.out.println("Total nodes of the PPCtree: " + ppcTree.countNodes());
        
        this.selector_nlists = ppcTree.create_Nlist_for_selectors_arr(this.constructing_selector_count);
        this.selector_nlist_map = ppcTree.create_selector_Nlist_map(this.selector_nlists);
        
        System.out.println("\n\nBenchmark memory for PPCtree, encoded instances from data, Nlists");
        
        String[] outputs;
        double prv_memory, memory;
        double mb = 1024*1024;
        
        System.out.println("\nBegin:");
        outputs = MemoryHistogramer.get_memory_histogram("core");
        System.out.println(outputs[0]);
        System.out.println(outputs[1]);
        prv_memory = get_total_memory(outputs[2]);
        
        ppcTree.free();
        ppcTree = null;
        System.out.println("\nWithout PPCtree:");
        outputs = MemoryHistogramer.get_memory_histogram("core");
        System.out.println(outputs[0]);
        System.out.println(outputs[1]);
        memory = get_total_memory(outputs[2]);
        System.out.println("Memory Difference: " + (prv_memory - memory)/mb + " MB");
        prv_memory = memory;
        
        for(int i=0; i<this.selectorID_records.length; i++) this.selectorID_records[i] = null;
        this.selectorID_records = null;
        MemoryHistogramer.force_garbage_collection();
        System.out.println("\nWithout encoded instance:");
        outputs = MemoryHistogramer.get_memory_histogram("core");
        System.out.println(outputs[0]);
        System.out.println(outputs[1]);
        memory = get_total_memory(outputs[2]);
        System.out.println("Memory Difference: " + (prv_memory - memory)/mb + " MB");
        prv_memory = memory;
        
        for(int i=0; i<this.selector_nlists.length; i++) this.selector_nlists[i] = null;
        this.selector_nlists = null;
        this.selector_nlist_map.clear();
        this.selector_nlist_map = null;
        MemoryHistogramer.force_garbage_collection();
        System.out.println("\nWithout Nlists:");
        outputs = MemoryHistogramer.get_memory_histogram("core");
        System.out.println(outputs[0]);
        System.out.println(outputs[1]);
        memory = get_total_memory(outputs[2]);
        System.out.println("Memory Difference: " + (prv_memory - memory)/mb + " MB\n\n");
        System.out.println("tree_time :" +  tree_time);
    }
    
	/**
	 * Benchmark consumed memory for P3Ctrees
	 * @param file_name
	 * @throws IOException
	 * @throws DataFormatException
	 * @return the maximum number of nodes of the P3Ctree
	 */
    public long benchmark_memory_for_p3ctree(String file_name) throws IOException, DataFormatException {
        this.data_filename = file_name;
        long a, tree_time = 0, n_list_time = 0;
        a = this.preprocessing();
        
        System.out.println("\n\nBenchmark memory for P3CTree");
        System.out.println("Memory efficiency: " + this.efficiency);
        String[] outputs;
        double begin_memory, mem_diff, max_mem_diff;
        double mb = 1024*1024;
        
        System.out.println("\nBegin:");
        outputs = MemoryHistogramer.get_memory_histogram("core");
        System.out.println(outputs[0]);
        System.out.println(outputs[1]);
        begin_memory = get_total_memory(outputs[2]);
        System.out.println("Consumed memory: " + (begin_memory/mb) + " MB");
        
        // Build the top part of the global PPCtree
        // Encoded instances are also created
        long start = System.currentTimeMillis();
        P3CTree p3ctree = new P3CTree(this.constructing_selector_count); 
        this.construct_tree_top_part(p3ctree);
        System.out.println("\nWith top part of the global tree and instances from data:");
        outputs = MemoryHistogramer.get_memory_histogram("core");
        System.out.println(outputs[0]);
        System.out.println(outputs[1]);
        max_mem_diff = (get_total_memory(outputs[2]) - begin_memory)/mb;
        System.out.println("Memory Difference: " + max_mem_diff + " MB");

        List<PPCNode> leaf_nodes = p3ctree.getLeafNodes();
        int subtree_number = 1;
        double min_count = 0.2* this.row_count/this.efficiency;
        System.out.println(min_count);
        long max_node_count = 0;
        tree_time +=  System.currentTimeMillis() - start;
        for (PPCNode leaf_node : leaf_nodes){
        	start = System.currentTimeMillis();
        	int level = ((P3CNode) leaf_node).instGroup.level;
        	// Build a subtree with root at leaf_node
        	p3ctree.buildSubtree(leaf_node);
        	
        	// Assign pre-order and post-order codes
        	p3ctree.assignPrePosOrderCodeSubTree(leaf_node);
        	tree_time +=  System.currentTimeMillis() - start;
        	start = System.currentTimeMillis();
        	// Update Nlist of selectors and free the subtree
        	p3ctree.update_nlists_from_subtree(leaf_node); 

        	if (leaf_node.count > min_count){
        		// Do not need to measure for so small subtrees
        		System.out.println("\nFrom number of instances: " + leaf_node.count);
                System.out.println("Subtree " + subtree_number 
                		+ " (level " + level + ") built and the current Nlists updated:");
                mem_diff = (MemoryHistogramer.get_memory_sum() - begin_memory)/mb;
                if (max_mem_diff < mem_diff) {
                	max_mem_diff = mem_diff;
                    this.p3cTotAbsMemoryMB = begin_memory/mb + max_mem_diff;
                }
                long node_count = p3ctree.countNodes();
                
                if (max_node_count < node_count) max_node_count = node_count;
                System.out.println("Memory Difference: " + mem_diff + " MB");
                System.out.println("Current node count: " + node_count);
        	}
        	
        	// Free the subtree with root at leaf_node for memory
        	p3ctree.freeSubTrees(leaf_node);
        	n_list_time += System.currentTimeMillis() - start;
        	subtree_number++;
        }
        // recommend a further efficiency coefficient based on 
        // the max number of instances used to build a subtree
        int max_inst_count = 0;
        for(PPCNode node : leaf_nodes){
        	if (max_inst_count < node.count) max_inst_count = node.count;
        }
        this.furtherEfficiency = this.row_count/(max_inst_count/2);
        start = System.currentTimeMillis();
        p3ctree.shrink_nlists();
       
        this.selector_nlists = p3ctree.get_selector_nlists();
        this.selector_nlist_map = p3ctree.create_selector_Nlist_map(this.selector_nlists);
        n_list_time += System.currentTimeMillis() - start;
        this.p3cTotMemoryMB = max_mem_diff;
        this.p3cTotMB = computeP3CTotFromHistogram();
        System.out.println("P3C Tot (sum of P3C-related classes): " + this.p3cTotMB + " MB");
        System.out.println("\n\n--------------------------------------------");
        System.out.println("The number of subtrees: " + leaf_nodes.size());
        System.out.println("Max node count: " + max_node_count);
    	System.out.println("Max Memory Difference: " + max_mem_diff + " MB (include: a subtree built upon the top part, current Nlists, instances from data)");
    	System.out.println("Max instance count used to build a subtree: " + max_inst_count);
    	System.out.println("Used efficiency: " + this.efficiency);
    	System.out.println("Further recommended efficiency: " + this.furtherEfficiency);
    	System.out.println("Preprocessing time: " + a + " ms");
		System.out.println("tree construction time: " + tree_time + " ms");
		System.out.println("n_list construction time: " + n_list_time + " ms");

    	return max_node_count;
    }
    private static final Pattern HISTO_LINE =
            Pattern.compile("\\s*\\d+:\\s+(\\d+)\\s+(\\d+)\\s+(.+)");

    /** 判斷某個 class 名稱是不是「我們要算進 P3C Tot 的結構」 */
    /** 判斷某個 class 名稱是不是「我們要算進 P3C Tot 的結構」 */
    private boolean isP3CRelatedClass(String className) {
        // 1) 直接就是 tree / N-list / encoded data 的 class
        if (className.startsWith("core.structure.")) return true;   // P3C / PPC nodes
        if (className.startsWith("core.prepr."))     return true;   // Selector / encoded data
        if (className.startsWith("nlistbase."))      return true;   // InfoBase / N-lists

        // 2) 這些是「陣列類型」，用來裝上面那些 class 或 tid / selector 的 primitive 陣列
        //    例如 [I = int[]、[J = long[]、[Lcore.prepr.Selector; = Selector[]
        if (className.equals("[I")    || className.equals("[[I"))    return true;  // int[], int[][]
        if (className.equals("[J")    || className.equals("[[J"))    return true;  // long[], long[][]
        if (className.startsWith("[Lcore.structure.")) return true;  // array of tree nodes
        if (className.startsWith("[Lcore.prepr."))     return true;  // array of prepr objects
        if (className.startsWith("[Lnlistbase."))      return true;  // array of N-list objects

        return false;
    }

    /**
     * 從完整的 GC.class_histogram 裡，把「與 P3C 有關的 class」的 bytes 加總。
     * 這裡的 P3C 包含：
     *  - core.structure.*       (tree nodes)
     *  - nlistbase.*           (InfoBase / N-lists)
     *  - core.prepr.*          (encoded data)
     *  - 它們的各種 array 類型 + int[] / long[]
     *
     * @return P3C Tot，單位 MB
     */
    private double computeP3CTotFromHistogram() throws IOException {
        String[] outputs = MemoryHistogramer.get_memory_histogram_all();
        String report = outputs[1];  // 全部內容（header + body + summary）

        long bytesSum = 0L;

        for (String line : report.split("\\R")) {  // \\R = 任意換行
            line = line.trim();
            if (line.isEmpty()) continue;

            Matcher m = HISTO_LINE.matcher(line);
            if (!m.matches()) {
                // header / summary 等不符合格式的行直接略過
                continue;
            }

            long bytes = Long.parseLong(m.group(2));    // 第二個 group = #bytes
            String className = m.group(3).trim();      // 第三個 group = class name

            if (isP3CRelatedClass(className)) {
                bytesSum += bytes;
            }
        }

        double mb = 1024.0 * 1024.0;
        return bytesSum / mb;
    }

    
    /**
	 * Benchmark consumed memory for encoded instances from data, Nlists of selectors. 
	 * Using this when the PPCtree gets memory overflow.
	 * @param file_name
	 * @throws IOException
	 * @throws DataFormatException
	 */
	public void benchmark_memory_for_encodeddata_nlists(String file_name) throws IOException, DataFormatException {
		this.data_filename = file_name;
		
		System.out.println("\n\nBenchmark memory for encoded instances from data, Nlists (Using this when PPC-tree was memory overflow)");
		
		this.fetch_information_with_memory_efficiency(data_filename);	// default efficiency = 1000
        
        String[] outputs;
        double prv_memory, memory;
        double mb = 1024*1024;
        
        System.out.println("\nBegin:");
        outputs = MemoryHistogramer.get_memory_histogram("core");
        System.out.println(outputs[0]);
        System.out.println(outputs[1]);
        prv_memory = get_total_memory(outputs[2]);
        
        for(int i=0; i<this.selectorID_records.length; i++) this.selectorID_records[i] = null;
        this.selectorID_records = null;
        MemoryHistogramer.force_garbage_collection();
        System.out.println("\nWithout encoded instance:");
        outputs = MemoryHistogramer.get_memory_histogram("core");
        System.out.println(outputs[0]);
        System.out.println(outputs[1]);
        memory = get_total_memory(outputs[2]);
        System.out.println("Memory Difference: " + (prv_memory - memory)/mb + " MB");
        prv_memory = memory;
        
        for(int i=0; i<this.selector_nlists.length; i++) this.selector_nlists[i] = null;
        this.selector_nlists = null;
        this.selector_nlist_map.clear();
        this.selector_nlist_map = null;
        MemoryHistogramer.force_garbage_collection();
        System.out.println("\nWithout Nlists:");
        outputs = MemoryHistogramer.get_memory_histogram("core");
        System.out.println(outputs[0]);
        System.out.println(outputs[1]);
        memory = get_total_memory(outputs[2]);
        System.out.println("Memory Difference: " + (prv_memory - memory)/mb + " MB\n\n");
        prv_memory = memory;
    }
    
    private double get_total_memory(String sum_string){
    	String[] items = sum_string.split(" ");
    	System.out.println(Double.parseDouble(items[items.length-1]));
    	return Double.parseDouble(items[items.length-1]);
    }
}
