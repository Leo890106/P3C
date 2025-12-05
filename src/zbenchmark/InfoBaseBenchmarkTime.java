package zbenchmark;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Paths;
import java.util.zip.DataFormatException;

import nlistbase.InfoBase;
import core.structure.INlist;

public class InfoBaseBenchmarkTime {
	
	public static void main(String[] args) throws IOException, DataFormatException {
		String output_dir = "data/output/";
		
		String[] data_filenames = new String[]{
//				"data/input/adult.arff",
				"data/input/online_retail_II.csv"
		};
		
		boolean use_fix_efficiencies = false;
		int[] efficiencies = new int[]{10, 20, 30, 40, 50, 100, 150, 200, 250, 300, 350, 400};
		int start_efficiency = 10;
		int upper_efficiency = 4000;
		
		// Using list of arguments for input data file names
		if (args.length > 2){
			use_fix_efficiencies = Boolean.parseBoolean(args[0]);
			upper_efficiency = Integer.parseInt(args[1]);
			data_filenames = new String[args.length-2];
			for(int i=2; i<args.length; i++){
				data_filenames[i-2] = args[i];
			}
		}
		
		if (use_fix_efficiencies){
			for (String data_filename : data_filenames){
				run(data_filename, efficiencies, output_dir);
			}
		}else{
			for (String data_filename : data_filenames){
				run(data_filename, start_efficiency, upper_efficiency, output_dir);
			}
		}
	}
	
	private static void run(String data_filename, int[] efficiencies, String output_dir) throws IOException, DataFormatException{
		String name = (Paths.get(data_filename).getFileName().toString().split("\\."))[0] + "_time_benchmark.txt";
		String output_filename = Paths.get(output_dir, name).toString();
		
		PrintStream out = new PrintStream(new FileOutputStream(output_filename));
		System.setOut(out);
		
		INlist[] nlists = null;
		try{
			long start = System.currentTimeMillis();
			InfoBase ibase1 = new InfoBase();
			ibase1.fetch_information(data_filename);
			nlists = ibase1.getSelectorNlists();
			long p3ctrees_runtime = System.currentTimeMillis() - start;
			
			System.out.println(data_filename);
			System.out.println(output_filename);
			System.out.println("PPCTree runtime: " + p3ctrees_runtime + " ms");
			
			//ibase1.export_nlists("data/output/nlists1.txt");
		}catch (OutOfMemoryError e){
			System.out.println("PPCTree was overflow!");
		}
		
		System.out.println("\n--------------------------------------------------\n");
		
		for (int efficiency : efficiencies){
			long start = System.currentTimeMillis();
			InfoBase ibase2 = new InfoBase();
			ibase2.setEfficiency(efficiency);
			ibase2.fetch_information_with_memory_efficiency(data_filename);
			INlist[] nlists2 = ibase2.getSelectorNlists();
			long subtrees_runtime = System.currentTimeMillis() - start;
			
			System.out.println("\nUsed efficiency: " + efficiency);
			System.out.println("P3CTrees runtime: " + subtrees_runtime + " ms");
			//ibase2.export_nlists("data/output/nlists2.txt");
			
			if (nlists != null){
				if (is_identical(nlists, nlists2))
					System.out.println("Two Nlists are identical");
				else
					System.out.println("Two Nlist are NOT identical");
			}else{
				System.out.println("No NLists matching because PPCTree was overflow!");
			}
			
			System.out.println("\n--------------------------------------------------");
		}
	}
	
	private static void run(String data_filename, 
							int start_efficiency, 
							int upper_efficiency, 
							String output_dir) throws IOException, DataFormatException{
		String name = (Paths.get(data_filename).getFileName().toString().split("\\."))[0] + "_time_benchmark.txt";
		String output_filename = Paths.get(output_dir, name).toString();
		
		PrintStream out = new PrintStream(new FileOutputStream(output_filename));
		System.setOut(out);
		
		INlist[] nlists = null;
		try{
			long start = System.currentTimeMillis();
			InfoBase ibase1 = new InfoBase();
			ibase1.fetch_information(data_filename);
			nlists = ibase1.getSelectorNlists();
			long ppctree_runtime = System.currentTimeMillis() - start;
			
			System.out.println(data_filename);
			System.out.println(output_filename);
			System.out.println("PPCTree runtime: " + ppctree_runtime + " ms");
			System.out.println("\n--------------------------------------------------\n");
			//ibase1.export_nlists("data/output/nlists1.txt");
		}catch (OutOfMemoryError e){
			System.out.println("PPCTree was overflow!");
		}
		
		
		int efficiency = start_efficiency;
		while(true){
			long start = System.currentTimeMillis();
			InfoBase ibase2 = new InfoBase();
			ibase2.setEfficiency(efficiency);
			ibase2.fetch_information_with_memory_efficiency(data_filename);
			INlist[] nlists2 = ibase2.getSelectorNlists();
			long p3ctrees_runtime = System.currentTimeMillis() - start;
			
			System.out.println("\nUsed efficiency: " + efficiency);
			System.out.println("P3CTrees runtime: " + p3ctrees_runtime + " ms");
			//ibase2.export_nlists("data/output/nlists2.txt");
			
			if (nlists != null){
				if (is_identical(nlists, nlists2))
					System.out.println("Two Nlists are identical");
				else
					System.out.println("Two Nlist are NOT identical");
			}else{
				System.out.println("No NLists matching because PPCTree was overflow!");
			}
			
			int eff = ibase2.getFurtherEfficiency();
			if (eff <= efficiency){
				efficiency = efficiency*2;
			}else{
				efficiency = eff;
			}
			if(efficiency > upper_efficiency) break;
			
			System.out.println("\n--------------------------------------------------");
		}
	}
	
	private static boolean is_identical(INlist[] nlists1, INlist[] nlists2){
		if (nlists1.length != nlists2.length) return false;
		
		for(int i=0; i<nlists1.length; i++){
			if (nlists1[i].isIdentical(nlists2[i])) continue;
			else return false;
		}
		
		return true;
	}
}
