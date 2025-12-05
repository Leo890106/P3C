package zbenchmark;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Paths;
import java.util.List;
import java.util.zip.DataFormatException;

import nlistbase.InfoBase;
import core.prepr.Attribute;
import core.structure.Supporter;

public class DistinctValueCountPerAttribute {
	
	public static void main(String[] args) throws IOException, DataFormatException {
		String output_dir = "data/output/";
		
		String[] data_filenames = new String[]{
//				"data/input/adult.arff",
				"data/input/online_retail_II.csv"
		};
		
		// Using list of arguments for input data file names
		if (args.length > 0) data_filenames = args;
		
		String output_filename = Paths.get(output_dir, "distinct_value_count_per_attribute.txt").toString();
		PrintStream out = new PrintStream(new FileOutputStream(output_filename));
		System.setOut(out);
		
		for (String data_filename : data_filenames){
			run(data_filename);
		}
	}
	
	private static void run(String data_filename) throws IOException, DataFormatException{
		System.out.println("\n-------------------------\nDataset file path: " + data_filename);
		
		InfoBase ibase = new InfoBase();
//		ibase.fetch_information(data_filename);		// use PPCtree
		ibase.fetch_information_with_memory_efficiency(data_filename);	// use P3Ctree
		List<Attribute> attrs = ibase.getAttributes();
		double[] dist_val_counts = new double[attrs.size()];
		for(int i=0; i<dist_val_counts.length; i++){
			dist_val_counts[i] = attrs.get(i).distinct_values.size();
		}
		double[] info = Supporter.get_statistic_info(dist_val_counts);
		
		System.out.println("max: " + info[0]);
		System.out.println("min: " + info[1]);
		System.out.println("mean: " + info[2]);
		System.out.println("std: " + info[3]);
		System.out.println("instance#: " + ibase.getRowCount());
	}
}
