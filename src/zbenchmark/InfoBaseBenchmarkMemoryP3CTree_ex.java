package zbenchmark;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Paths;
import java.util.zip.DataFormatException;

import nlistbase.InfoBase;

public class InfoBaseBenchmarkMemoryP3CTree_ex {
	
	public static void main(String[] args) throws IOException, DataFormatException{
		String output_dir = "data/output/";
		
		int start_efficiency;
		String data_filename;
		
		if (args.length == 2){
			start_efficiency = Integer.parseInt(args[0]);
			data_filename = args[1];
		}else{
			return;
		}
		
		// test for one dataset with one e value
		run(data_filename, start_efficiency, output_dir);
	}
	
	private static long[] run(String data_filename, int efficiency, String output_dir) throws IOException, DataFormatException{
		String name = (Paths.get(data_filename).getFileName().toString().split("\\."))[0] + "_memory_benchmark_p3ctree_effc" + efficiency + ".txt";
		String output_filename = Paths.get(output_dir, name).toString();
		
		PrintStream out = new PrintStream(new FileOutputStream(output_filename));
		System.setOut(out);
		
		InfoBase ibase = new InfoBase();
		ibase.setEfficiency(efficiency);
		long max_node_count = ibase.benchmark_memory_for_p3ctree(data_filename);
		int eff = ibase.getFurtherEfficiency();
		out.flush();
		
		return new long[]{eff, max_node_count};
	}
}
