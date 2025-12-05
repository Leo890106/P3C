/*
 * @author Van Quoc Phuong Huynh, FAW JKU
 *
 */

package core.structure;

import java.util.List;


/**
 * P3CNode extends class PPCNode that constructs the top part of a PPCTree.
 * </br>It introduces a new property, a group of instances which is used to
 * build up a subtree with root node at a leaf node.
 * </br>Only leaf nodes have a corresponding instances group.
 */
public class P3CNode extends PPCNode {
	public InstGroup instGroup = null;
    
	public P3CNode(){
		super();
	}
			
    public P3CNode(int item_id, PPCNode parent, int count, int level, List<int[]> instances) {
    	super(item_id, parent, count);
    	this.instGroup = new InstGroup(level, instances);
    }
}
