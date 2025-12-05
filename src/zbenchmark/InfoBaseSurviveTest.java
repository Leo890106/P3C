package zbenchmark;

import java.io.IOException;
import java.util.zip.DataFormatException;

import nlistbase.InfoBase;

public class InfoBaseSurviveTest {
	
	public static void main(String[] args) throws IOException, DataFormatException {
		if (args.length != 2){
			System.out.println("parameters: <path_to_input_file> <efficiency>");
			System.out.println("If <efficiency> <= 1, do survive test with PPC-tree");
			return;
		}
		
		String data_filename = args[0];
		int efficiency = Integer.parseInt(args[1]);
		
		long start = System.currentTimeMillis();
		InfoBase ibase = new InfoBase();
		ibase.setEfficiency(efficiency);
		
		if (efficiency <= 1){
			System.out.println("Test with PPC-tree");
			ibase.fetch_information(data_filename); // with PPC-tree
		}else{
			System.out.println("Test with P3C-tree, efficiency=" + efficiency);
			ibase.fetch_information_with_memory_efficiency(data_filename);	// with P3C-tree
		}
		
		long runtime = System.currentTimeMillis() - start;
		System.out.println("Runtime: " + runtime + " ms");
	}
}
