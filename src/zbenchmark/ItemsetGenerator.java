/*
 * @author Van Quoc Phuong Huynh, FAW JKU
 *
 */

package zbenchmark;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;

/***
 * This class provides a method to generate a number of random itemsets reproducibly with min and max length
 *
 */
public class ItemsetGenerator {	
	
	protected static int[] gen_random_itemset(int[] example, Random random, int length){
		if (example.length < length){
			return null;
		}else if (example.length == length){
			return example.clone();
		}
		
		HashSet<Integer> itemset = new HashSet<Integer>();
		int[] itemset_arr = new int[length];
		int ex_index_upper = example.length - 1;
		int i = 0;
		while(i < itemset_arr.length -1){
			int item = example[random.nextInt(ex_index_upper)];
			if (itemset.contains(item)){
				continue;
			}else{
				itemset.add(item);
				itemset_arr[i] =  item;
				i++;
			}
		}
		itemset_arr[itemset_arr.length -1] = example[example.length -1];
		Arrays.sort(itemset_arr);
		return itemset_arr;
	}
	
	public static int[][] gen_random_itemsets(int[][] encoded_data, 
												double count,
												int min_length,
												int max_length,
												int seed){
		Random random = new Random(seed);
		
		// calculate how many itemsets generated per instance
		double n_itemset_per_row = count/encoded_data.length;
		int n_itemset_floor = (int) Math.floor(n_itemset_per_row);
		int step;
		if (n_itemset_per_row == n_itemset_floor){
			step = Integer.MAX_VALUE;
		}else{
			step = (int) (1/(n_itemset_per_row - n_itemset_floor));
		}
		int[][] itemsets = new int[(int) count][];

		int index = 0;
		for(int i=0; i<encoded_data.length; i++){
			int[] example = encoded_data[i];
			int n;
			if (i % step == 0){
				n = n_itemset_floor + 1;
			}else{
				n = n_itemset_floor;
			}
			for(int k=0; k<n; k++){
				int length = get_random_length(random, min_length, max_length);
				int[] itemset = gen_random_itemset(example, random, length);
				if(itemset != null && index < count){
					itemsets[index] = itemset;
					index++;
				}
			}
		}
		
		// try generate random itemsets to meet "count" itemsets
		while(index < itemsets.length){
			int[] example = encoded_data[random.nextInt(encoded_data.length)];
			int length = get_random_length(random, min_length, max_length);
			int[] itemset = gen_random_itemset(example, random, length);
			if(itemset != null){
				itemsets[index] = itemset;
				index++;
			}
		}
		
		return itemsets;
	}
	
	protected static int get_random_length(Random random, int min, int max){
		double offset = 2.0; // Mean of the distribution
		double stdDev = 1.5; // Standard deviation of the distribution
		double randomNumber;
		do {
            randomNumber = random.nextGaussian() * stdDev + offset;
        } while (randomNumber < min || randomNumber > max); // the generated number is within range [min, max]
		return (int) Math.round(randomNumber);
	}
}
