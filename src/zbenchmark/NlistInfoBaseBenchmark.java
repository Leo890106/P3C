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

import nlistbase.InfoBase;
import core.structure.INlist;

public class NlistInfoBaseBenchmark {
	
	public static void main(String[] args) throws IOException, DataFormatException {
		String output_dir = "data/output/";
		
		String[] data_filenames = new String[]{
				//"data/input/adult.arff",
				"data/input/connect-4.csv"
		};
		
		// Using list of arguments for input data file names
		// args: start with two optional numbers: efficiency value, number of random itemset, then followed with file paths
		if (args.length > 0) data_filenames = args;
		
		int efficiency = 10;
		int n_itemsets = 1000000;	// used for the number of random itemsets
		int min_length = 2;
		int max_length = 8;
		int seed = 0;	// for reproducibility
		int index = 0;
		for (String str_data : data_filenames){
			try{
				if(index == 0) {
					efficiency = Integer.parseInt(str_data);
					index ++;
				}else{
					n_itemsets = Integer.parseInt(str_data);
				}
				continue;
			}catch(NumberFormatException e){}
			// str_data is a file path to a dataset
			run(str_data, efficiency, n_itemsets, min_length, max_length, seed, output_dir);
		}
	}
	
	private static void run(String data_filename,
							int efficiency,
							int n_itemsets, 
							int min_length,
							int max_length,
							int seed,
							String output_dir) throws IOException, DataFormatException{
		String name = (Paths.get(data_filename).getFileName().toString().split("\\."))[0] + "_nlist.txt";
		String output_filename = Paths.get(output_dir, name).toString();
		
		PrintStream out = new PrintStream(new FileOutputStream(output_filename));
		System.setOut(out);
		
		InfoBase ibase = new InfoBase();
		ibase.setEfficiency(efficiency);
		long[] times = ibase.fetch_information_with_memory_efficiency(data_filename);
		System.out.println("Preprocessing time: " + times[0] + " ms");
		System.out.println("Nlist construction time: " + (times[1]+times[2]) + " ms");
		
		// calculate average length of the basic Nlists
		double avg_length = 0;
		for(INlist basic_nlist : ibase.getSelectorNlists()){
			avg_length += basic_nlist.size();
		}
		avg_length = avg_length/ibase.getSelectorNlists().length;
		System.out.println("Average length of basic Nlists: " + avg_length);
		
		//Generate random itemsets
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
        System.out.println("Average length: " + mean/itemsets.length);
		
		//Benchmark runtime of generating NLISTs for random itemsets
		long start = System.currentTimeMillis();
		for(int[] itemset : itemsets){
			ibase.create_nlist_for_itemset(itemset);
		}
		long duration = System.currentTimeMillis() - start;
		System.out.println("\nTime for generating NLISTs for " + itemsets.length + " random itemsets: " + duration + " ms");
	}
}
