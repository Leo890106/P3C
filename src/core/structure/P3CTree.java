/*
 * @author Van Quoc Phuong Huynh, FAW JKU
 *
 */

package core.structure;

import java.util.ArrayList;
import java.util.List;

import core.prepr.IntegerArray;

/**
 * P3CTree (Partial PrePostCode tree) for generating memory-efficiently Nlist of items or selectors.
 * </br></br>Each subtree is built up on the top part of the global PPCTree, each time only one subtree mounted on the global PPCtree. 
 * Then Nlist of selectors are updated from the subtree's PPCNodes which are freed before another subtree. 
 */
public class P3CTree extends PPCTree {
	private List<PPCNode> leafNodes;
	private INlist[] selector_nlists;
	
	////////////////////////////////////////////// COMMONS METHODS //////////////////////////////////////////////////

	public P3CTree(int selector_count) {
		this.currentPreCode = 0;
		this.currentPosCode = 0;
		this.root = new P3CNode();
		this.selector_nlists = this.init_selector_nlist(selector_count);
	}
	private INlist[] init_selector_nlist(int selector_count){
		// Initialize 'selector_nlists', add an empty Nodelist for each selector.
    	INlist[] selector_nlists = new INlist[selector_count];
    	for(int i=0; i<selector_count; i++){
    		selector_nlists[i] = new Nodelist();
    	}
    	return selector_nlists;
	}
	
	public INlist[] get_selector_nlists(){
		return this.selector_nlists;
	}	
	
	public List<PPCNode> getLeafNodes(){
		if (this.leafNodes == null){
			this.leafNodes = this.collectLeafNodes();
		}
		return this.leafNodes;
	}
	private List<PPCNode> collectLeafNodes(){
		List<PPCNode> leafNodes = new ArrayList<PPCNode>();
		for (PPCNode node : this.root.children){
			if (node.children.size() == 0){
				leafNodes.add(node);
			}else{
				this.collectLeafNodesRecursive(node, leafNodes);
			}
		}
		return leafNodes;
	}
	private void collectLeafNodesRecursive(PPCNode node, List<PPCNode> leafNodes){
		for (PPCNode child : node.children){
			if (child.children.size() == 0){
				leafNodes.add(child);
			}else{
				this.collectLeafNodesRecursive(child, leafNodes);
			}
		}
	}
	
	/**
	 * Return an array of selector IDs associated with nodes in the path from 'sub_node' to the root
	 * @param sub_node
	 * @return
	 */
	public int[] getPrefixIDs(PPCNode sub_node){
		IntegerArray prefix_ids = new IntegerArray();
		PPCNode node = sub_node;
		while (node.parent != null){
			prefix_ids.add(node.itemID); // selector ID
			node = node.parent;
		}
		return prefix_ids.toArray();
	}
	
	/**
	 * Build up a subtree at node 'sub_node' from the suffix of the input instances. 
	 * 'sub_node' includes the reference to the instances to build the subtree
	 * @param sub_node is a leaf node of the top part of the global tree, be the root of the sub tree to build up
	 */
	public void buildSubtree(PPCNode sub_node){
		P3CNode subroot = (P3CNode) sub_node;
		int level = subroot.instGroup.level;
		List<int[]> instances = subroot.instGroup.instances;
		if (instances == null) return;
		
		for (int[] instance : instances){
			this.insert_record(sub_node, instance, level);
		}
		
		// now all instances at 'sub_node' are no longer used and freed
		instances.clear();
		subroot.instGroup.instances = null;
	}
	private void insert_record(PPCNode sub_node, int[] record, int level){
	    PPCNode new_node, mid_child;
	    boolean wasNotMerged;
	    int id, position, mid_index, size;
	
	    // The record of ids is in ascending order.
	    // So the order of ids to insert into the tree is from right to left.
	    for(int i = record.length-level; i>-1; i--){
	    	id = record[i];
	        wasNotMerged = true;
	        position = 0;
	    	size = sub_node.children.size();
	    	
	    	// Binary search on the id-based ordered children node list of sub_node
	    	while (position < size) {
	    		mid_index = (position + size) / 2;
	            mid_child = sub_node.children.get(mid_index);
	            
	            if (mid_child.itemID < id) position = mid_index + 1;
	            else if (mid_child.itemID > id) size = mid_index;
	            else {
	            	mid_child.count++;
	            	sub_node = mid_child;
	                wasNotMerged = false;
	                break;
	            }
	        }
	        
	        if (wasNotMerged) {
	        	new_node = new PPCNode(id, sub_node, 1);
	        	// position now is the right index in children node list of sub_node
	        	sub_node.children.add(position, new_node);
	        	sub_node = new_node;
	        }
	    }
	}
	
	/**
	 * Build the top part of the global tree from all instances in the input data that
	 * every subtree with root at leaf node (of the top part) will be built from a number
	 * of instances not exceed 'max_inst_count'.
	 * </br>Leaf nodes of the top part will be at different levels.
	 * @param data_instances
	 * @param max_inst_count
	 */
	public void buildTopPart(int[][] data_instances, long max_inst_count){
		this.growAtRootOnelevel(data_instances);
		this.buildTopPartRecursive(this.root, max_inst_count);
	}
	/**
	 * Grow at root of the SubPPCTree one level from all instances from the input data, 
	 * build its child nodes.
	 * @param data_instances
	 */
	private void growAtRootOnelevel(int[][] data_instances){
		PPCNode root_node = this.root;

		for (int[] instance : data_instances){
			this.grow(root_node, instance, 1);
		}
	}
	private void buildTopPartRecursive(PPCNode sub_node, long max_inst_count){
		for(PPCNode child : sub_node.children){
			if (child.count > max_inst_count) {
				this.growAtNodeOnelevel(child);
				this.buildTopPartRecursive(child, max_inst_count);
			}	
		}
	}
	/**
	 * Grow at the 'sub_node' one more level, build its child nodes.
	 * </br>After finishing, the list of instances at 'sub_node' will be free 
	 * because they had been split and transfered to its child nodes, 
	 * @param sub_node
	 */
	private void growAtNodeOnelevel(PPCNode sub_node){
		P3CNode subroot = (P3CNode) sub_node;
		int level = subroot.instGroup.level;
		List<int[]> instances = subroot.instGroup.instances;

		for (int[] instance : instances){
			this.grow(subroot, instance, level);
		}
		
		// now all instances at 'sub_node' had been split and transfered to its child nodes.
		instances.clear();
		subroot.instGroup.instances = null;
	}
	private void grow(PPCNode sub_node, int[] instance, int level){
	    PPCNode mid_child;
    	int id = instance[instance.length - level];
        int position = 0, mid_index;
    	int size = sub_node.children.size();
    	boolean wasNotMerged = true;
    	
    	// Binary search on the id-based ordered children node list of sub_node
    	while (position < size) {
    		mid_index = (position + size) / 2;
            mid_child = sub_node.children.get(mid_index);
            
            if (mid_child.itemID < id) position = mid_index + 1;
            else if (mid_child.itemID > id) size = mid_index;
            else {
            	mid_child.count++;
            	// only add the instance if it can be used to grow the tree further
            	if(instance.length > level) ((P3CNode) mid_child).instGroup.instances.add(instance);
                wasNotMerged = false;
                break;
            }
        }
        
        if (wasNotMerged) {
        	List<int[]> instances = new ArrayList<int[]>();
        	
        	// only add the instance if it can be used to grow the tree further
        	if(instance.length > level) instances.add(instance);
        	
        	// position now is the right index in children node list of sub_node
        	sub_node.children.add(position, new P3CNode(id, sub_node, 1, level+1, instances));
        }
	}
	
	/**
	 * Assign PPCode for nodes of the subtree with its root at 'sub_node'
	 * @param sub_node
	 */
	public void assignPrePosOrderCodeSubTree(PPCNode sub_node){		
		// assign pre-order code for ancestor nodes of sub_node
		// principle: if the first child is assigned a pre-order code, its parent must be assigned a pre-order code beforehand
		this.assignPreOrderCode_for_AncestorsWithoutPreOrderCode(sub_node);
		
		// assign pre-order and post-order codes for each node in the subtree
		this.traverseAssignPrePosOrderCode(sub_node);
		
		// assign post-order code for ancestor nodes of sub_node
		// principle: if the last child is assigned a post-order code, its parent must be assigned a post-order code also afterward
		this.assignPostOrderCode_for_Ancestors(sub_node);
	}
	private void assignPreOrderCode_for_AncestorsWithoutPreOrderCode(PPCNode sub_node){
		if (sub_node != sub_node.parent.children.get(0)) return; // check if sub_node is the first child		

		List<PPCNode> ancestors = new ArrayList<PPCNode>();
		PPCNode node = sub_node;
		// condition (node.parent.pre == -1) equals checking if 'node' is the first child of its parent
		while (node.parent != null && node.parent.pre == -1){
			ancestors.add(node.parent);
			node = node.parent;
		}
		
		for (int i=ancestors.size()-1; i>-1; i--){
			ancestors.get(i).pre = this.currentPreCode;
			this.currentPreCode ++;
		}
	}
    private void traverseAssignPrePosOrderCode(PPCNode tree_node){
    	// Assign a code for the current node
    	tree_node.pre = this.currentPreCode;
    	this.currentPreCode++;
    	
    	// If is not a leaf node, traverse all its children
    	for(PPCNode child : tree_node.children) traverseAssignPrePosOrderCode(child);
    	
    	tree_node.pos = this.currentPosCode;
		this.currentPosCode++;
    }
    private void assignPostOrderCode_for_Ancestors(PPCNode sub_node){
    	// check if sub_node is the last child of its parent
    	// sub_node.parent.itemID == -1 (happen if level=1), not assign post-order code for the root node (without a selector associated)
    	if (sub_node != sub_node.parent.children.get(sub_node.parent.children.size()-1) 
    			|| sub_node.parent.itemID == -1) return;
    	
    	//sub_node is the last child of its parent node 
    	PPCNode ancestor = sub_node.parent;
    	ancestor.pos = this.currentPosCode;
		this.currentPosCode ++;
		// ancestor has full codes, add it to the corresponding Nlist
		this.selector_nlists[ancestor.itemID].add(ancestor.pre, ancestor.pos, ancestor.count);
		
		while (ancestor.parent != null 
				&& ancestor.parent.itemID != -1 // not assign post-order code for the root node (without a selector associated)
				&& ancestor == ancestor.parent.children.get(ancestor.parent.children.size()-1)){
			ancestor = ancestor.parent;
			ancestor.pos = this.currentPosCode;
			this.currentPosCode ++;
			
			// ancestor has full codes, add it to the corresponding Nlist
			this.selector_nlists[ancestor.itemID].add(ancestor.pre, ancestor.pos, ancestor.count);
		}
	}
    
    /**
     * Update Nlists of selectors from nodes of the subtree with root at 'sub_node'
     * </br><b>Note:</b> After Nlist of selectors updated, the subtree should be freed for memory
     * by calling function freeSubTrees(PPCNode sub_node).
     * @param sub_node
     */
    public void update_nlists_from_subtree(PPCNode sub_node){
    	// Add root node of the subtree to the corresponding nlist
   	 	this.selector_nlists[sub_node.itemID].add(sub_node.pre, sub_node.pos, sub_node.count);
   	 
   	 	// Update selector_nlists
   	 	for(PPCNode child : sub_node.children){
   	 		this.update_nlists_recursive(child, this.selector_nlists);
   	 	}
    }
    private void update_nlists_recursive(PPCNode node, INlist[] selector_nlists){
    	// itemID of a TreeNode means Selector.selectorID
    	selector_nlists[node.itemID].add(node.pre, node.pos, node.count);
    	
    	// Recursive call for child nodes
    	for(PPCNode child : node.children) update_nlists_recursive(child, selector_nlists);
    }
    
    /**
     * Free the subtree whose root is at sub_node
     * @param sub_node root node of the subtree
     */
	public void freeSubTrees(PPCNode sub_node){
		sub_node.children.clear();
		sub_node.children = null;
		
		// if the subtree is the last child in the children list of its parent
		// all the children list is freed
		List<PPCNode> children = sub_node.parent.children;
		if (sub_node == children.get(children.size()-1)){
			children.clear();
			sub_node.parent.children = null;
		}
	}
	
	/**
	 * Collect redundant memory that was allocated for Nlists
	 */
	public void shrink_nlists(){
		for (INlist nlist : this.selector_nlists){
			nlist.shrink();
		}
	}
}