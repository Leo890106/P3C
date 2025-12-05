package core.structure;

import java.util.List;

public class InstGroup {
	public int level;
	public List<int[]> instances;
	
	public InstGroup(int level, List<int[]> instances){
		this.level = level;
		this.instances = instances;
	}
}
