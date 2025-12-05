/*
 * @author Van Quoc Phuong Huynh, FAW JKU
 *
 */

package zbenchmark;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.zip.DataFormatException;

import nlistbase.MemoryHistogramer;
import tidsetbase.DiffsetInfoBase;
import tidsetbase.IntegerArray;

public class DiffsetInfoBaseBenchmark {
	
	public static void main(String[] args) throws IOException, DataFormatException{
		String output_dir = "data/output/";
		
		String[] data_filenames = new String[]{
//				"data/input/adult.arff",
				"data/input/connect-4.csv",
		};
		
		// Using list of arguments for input data file names
		// args: start with optional number of random itemset, then followed with file paths
		if (args.length > 0) data_filenames = args;
		
		int n_itemsets = 1000000;
		int min_length = 2;
		int max_length = 8;
		int seed = 0;	// for reproducibility
		for (String str_data : data_filenames){
			try{
				n_itemsets = Integer.parseInt(str_data);
				continue;
			}catch(NumberFormatException e){}
			// str_data is a file path to a dataset
			run(str_data, n_itemsets, min_length, max_length, seed, output_dir);
		}
	}
	
	private static void run(String data_filename,
							int n_itemsets, 
							int min_length,
							int max_length,
							int seed,
							String output_dir) throws IOException, DataFormatException{
		String name = (Paths.get(data_filename).getFileName().toString().split("\\."))[0] + "_diffset.txt";
		String output_filename = Paths.get(output_dir, name).toString();
		PrintStream out = new PrintStream(new FileOutputStream(output_filename, true));	// true for append mode
		System.setOut(out);
		
		DiffsetInfoBase ibase = new DiffsetInfoBase();
		long[] times = ibase.fetch_information(data_filename);
		System.out.println("Preprocessing time: " + times[0] + " ms");
		System.out.println("Diffsets construction time: " + times[1] + " ms");
//		ibase.export_basic_diffsets(output_filename);	// for test only
		
		// calculate average length of the basic Diffsets
		double avg_length = 0;
		for(IntegerArray tidset : ibase.getBasicDiffsets()){
			avg_length += tidset.size();
		}
		avg_length = avg_length/ibase.getBasicDiffsets().length;
		System.out.println("Average length of basic Diffset: " + avg_length);
		int[][] records = ibase.getSelectorIDRecords();
		int nonEmpty = 0;
		for (int[] r : records) if (r != null && r.length > 0) nonEmpty++;
		if (nonEmpty == 0) {
		    System.out.println("[ERROR] No non-empty examples available. " +
		            "Lower minsup or fix preprocessing, then retry.");
		    return; // 或 throw
		}
		// Generate random itemsets
		System.out.println("1");
		int[][] itemsets = ItemsetGenerator.gen_random_itemsets(ibase.getSelectorIDRecords(),
																n_itemsets, min_length, max_length, seed);
		// report frequency for each length value
		Map<Integer, Integer> length_freq = new HashMap<Integer, Integer>();
        for(int i=min_length; i<=max_length; i++){
        	length_freq.put(i, 0);
        }
        for(int[] itemset : itemsets){
        	length_freq.put(itemset.length, length_freq.get(itemset.length)+1);
        }
        System.out.println("\nInformation report on the random itemsets");
        double mean = 0;
        for(Entry<Integer, Integer> e : length_freq.entrySet()){
        	System.out.println("Length: "+e.getKey()+" frequency: "+e.getValue());
        	mean += e.getKey()*e.getValue();
        }
        System.out.println("Average length: " + mean/itemsets.length + "\n");

		//Benchmark runtime calculate support count for random itemsets based on DIFFSETs
//		System.out.println("\n------Random Itemsets------");	// for test only
		
		long start = System.currentTimeMillis();
		for(int[] itemset : itemsets){
			ibase.calculate_supportcount_for_itemset(itemset);
//			ibase.calculate_union_set(itemset);
			// For test only
//			IntegerArray union_set = ibase.calculate_union_set(itemset);
//			int sc1 = ibase.getRowCount() - union_set.size();
//			int sc2 = ibase.calculate_supportcount_for_itemset(itemset);
//			System.out.print(Arrays.toString(itemset));
//			if(sc1 == sc2) System.out.println(" Support-count: MATCHING " + sc1);
//			else System.out.println(" Support-count: " + sc1 + ", " + sc2);
		}
		long duration = System.currentTimeMillis() - start;
		System.out.println("\nTime for generating DIFFSETs for " + itemsets.length + " random itemsets: " + duration + " ms");
		
        // Benchmark consumed memory
		String[] outputs;
        double mb = 1024*1024;
        outputs = MemoryHistogramer.get_memory_histogram("tidsetbase");
        double begin_memory = get_total_memory(outputs[2]);
        System.out.println("\nBegin benchmark memory\n");
        System.out.println(outputs[0]);
        System.out.println(outputs[1]);
        System.out.println("Consumed memory with basic Diffsets: " + (begin_memory/mb) + " MB\n\n");
        
        ibase.free_basic_diffsets();
        outputs = MemoryHistogramer.get_memory_histogram("tidsetbase");
        ibase.getAttrCount();	// to avoid ibase being freed
        double after_memory = get_total_memory(outputs[2]);
        double mem_diff = (begin_memory - after_memory)/mb;
        System.out.println(outputs[0]);
        System.out.println(outputs[1]);
        System.out.println("Consumed memory without basic Diffsets: " + (after_memory/mb) + " MB");
        System.out.println("\nMemory difference: " + mem_diff + " MB");
	}
	
	private static double get_total_memory(String sum_string){
    	String[] items = sum_string.split(" ");
    	return Double.parseDouble(items[items.length-1]);
    }
}
