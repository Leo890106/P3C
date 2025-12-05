package zbenchmark;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Paths;
import java.util.zip.DataFormatException;

import nlistbase.InfoBase;

public class InfoBaseBenchmarkMemoryP3CTree {
	
	public static void main(String[] args) throws IOException, DataFormatException{
		String output_dir = "data/output/";
		
		String[] data_filenames = new String[]{
//				"data/input/avazu_all_sparse.arff",
				"data/input/train.csv"
		};
		
		boolean use_fix_efficiencies = false;
		int[] efficiencies = new int[]{10, 20, 30, 40, 50, 100, 150, 200, 250, 300, 350, 400};
		int start_efficiency = 10;
		
		// Using list of arguments for input data file names
		if (args.length > 1){
			use_fix_efficiencies = Boolean.parseBoolean(args[0]);
			data_filenames = new String[args.length-1];
			for(int i=1; i<args.length; i++){
				data_filenames[i-1] = args[i];
			}
		}
		
		if (use_fix_efficiencies){
			for(String data_filename : data_filenames){
				for(int efficiency : efficiencies){
					run(data_filename, efficiency, output_dir);
				}
			}
		}else{
			for(String data_filename : data_filenames){
				long prev_max_node_count = Long.MAX_VALUE;
				long[] eff_maxNodeCount = new long[]{start_efficiency, prev_max_node_count};
				int num = 0;

				while(true){
					eff_maxNodeCount = run(data_filename, eff_maxNodeCount[0], output_dir);
					// stop the experiment if the P3C-tree does not reduce its size (number of nodes) more
					System.out.println(eff_maxNodeCount[1] + "    " + prev_max_node_count);
					num++;
					if(num >= 10)
						break;
					//if(eff_maxNodeCount[1] > prev_max_node_count) break;
					//prev_max_node_count = eff_maxNodeCount[1];
				}
			}
		}
	}
	
	private static long[] run(String data_filename, long efficiency, String output_dir) throws IOException, DataFormatException{
		String name = (Paths.get(data_filename).getFileName().toString().split("\\."))[0] + "_memory_benchmark_p3ctree_effc" + efficiency + ".txt";
		String output_filename = Paths.get(output_dir, name).toString();
		
		PrintStream out = new PrintStream(new FileOutputStream(output_filename));
		System.setOut(out);
		
		InfoBase ibase = new InfoBase();
		ibase.setEfficiency(efficiency);
		long max_node_count = ibase.benchmark_memory_for_p3ctree(data_filename); 
		long eff = ibase.getFurtherEfficiency();
		if (eff == efficiency){
			eff = efficiency*2;
		}
		double p3cTotMB = ibase.getP3cTotMB();
		System.out.println("P3C Tot (from InfoBase) : " + p3cTotMB + " MB");
		return new long[]{eff, max_node_count};
	}
}
