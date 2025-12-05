package zbenchmark;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Paths;
import java.util.zip.DataFormatException;

import nlistbase.InfoBase;

public class InfoBaseBenchmarkMemoryPPCTree {
	
	public static void main(String[] args) throws IOException, DataFormatException{
		String output_dir = "data/output/";
		
		String[] data_filenames = new String[]{
//				"data/input/avazu_all_sparse.arff",
				"data/input/train.csv"
		};
		
		// Using list of arguments for input data file names
		if (args.length > 0) data_filenames = args;
		
		for (String data_filename : data_filenames){
			run(data_filename, output_dir);
		}
	}
	
	private static void run(String data_filename, String output_dir) throws IOException, DataFormatException{
		String name = (Paths.get(data_filename).getFileName().toString().split("\\."))[0] + "_memory_benchmark_ppctree.txt";
		String output_filename = Paths.get(output_dir, name).toString();
		
		PrintStream out = new PrintStream(new FileOutputStream(output_filename));
		System.setOut(out);
		
		InfoBase ibase = new InfoBase();
		ibase.benchmark_memory_for_ppctree(data_filename);
	}
}
