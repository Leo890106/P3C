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

import tidsetbase.IntegerArray;
import tidsetbase.TidsetInfoBase;

public class TidsetInfoBaseBenchmark {
	
	public static void main(String[] args) throws IOException, DataFormatException{
		String output_dir = "data/output/";
		
		String[] data_filenames = new String[]{
//				"data/input/adult.arff",
				"data/input/connect-4.csv"
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
		String name = (Paths.get(data_filename).getFileName().toString().split("\\."))[0] + "_tidset.txt";
		String output_filename = Paths.get(output_dir, name).toString();
		PrintStream out = new PrintStream(new FileOutputStream(output_filename, true));	// true for append mode
		System.setOut(out);
		
		TidsetInfoBase ibase = new TidsetInfoBase();
		long[] times = ibase.fetch_information(data_filename);
		System.out.println("Preprocessing time: " + times[0] + " ms");
		System.out.println("Tidset construction time: " + times[1] + " ms");
//		ibase.export_basic_tidsets(output_filename);	// for test only
		
		// calculate average length of the basic Tidsets
		double avg_length = 0;
		for(IntegerArray tidset : ibase.getBasicTidsets()){
			avg_length += tidset.size();
		}
		avg_length = avg_length/ibase.getBasicTidsets().length;
		System.out.println("Average length of basic Tidsets: " + avg_length);
		
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
        System.out.println("Average length: " + mean/itemsets.length + "\n");

		//Benchmark runtime of generating TIDSETs for random itemsets
//		System.out.println("\n------Random Itemsets------");	// for test only
		long start = System.currentTimeMillis();
		for(int[] itemset : itemsets){
			ibase.create_tidset_for_itemset(itemset);
			// For test only
//			IntegerArray tidset = ibase.create_tidset_for_itemset(itemset);
//			System.out.print(Arrays.toString(itemset));
//			System.out.println(" Support-count: " + tidset.size());
//			System.out.print(" tidset: ");
//			System.out.println(Arrays.toString(tidset.toArray()));
		}
		long duration = System.currentTimeMillis() - start;
		System.out.println("\nTime for generating TIDSETs for " + itemsets.length + " random itemsets: " + duration + " ms");
		
        // Benchmark consumed memory of the basic tidsets
		// The memory consumed by the basic Tidsets equals the memory consumed by the encoded data.
		// The encoded data is in Horizontal format, and Tidset is in Vertical format of the data source
	}
}
