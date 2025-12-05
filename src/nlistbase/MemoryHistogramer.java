/*
 * @author Van Quoc Phuong Huynh, FAW JKU
 *
 */

package nlistbase;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This class is used internally to measure exactly the used memory of objects of each class.
 * </br>The result is written down to file.
 */
public class MemoryHistogramer {
	
	/**
	 * Return the precise memory amount of objects of each class. It forces to run GC
	 * @return [header_string, report_string, summary_string]
	 * @throws IOException
	 */
	public static String[] get_memory_histogram_all() throws IOException {
	    String name = ManagementFactory.getRuntimeMXBean().getName();
	    String PID = name.substring(0, name.indexOf("@"));	// Process ID of the program calling this function

	    //GC.class_histogram parameter forces JVM runs garbage collection
	    Process p = Runtime.getRuntime().exec("jcmd " + PID + " GC.class_histogram");
	    
	    //-histo:live modifier forces JVM runs garbage collection
	    //Process p = Runtime.getRuntime().exec("jmap " + "-histo:live " + PID);
	    
	    StringBuffer sb = new StringBuffer(1024*4);
	    BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream()));
	    
	    List<Object> lines = input.lines().collect(Collectors.toList());
	    
	    for(Object line : lines){
	    	sb.append(line).append('\n');
	    }
    	
	    return new String[]{(String) lines.get(2), sb.toString(), (String) lines.get(lines.size()-1)};
	}
	
	/**
	 * Return the precise memory amount of objects of each class. It forces to run GC
	 * @param package_name help to filter classes belonging to the package
	 * @return [header_string, report_string, summary_string]
	 * @throws IOException
	 */
	public static String[] get_memory_histogram(String package_name) throws IOException {
	    String name = ManagementFactory.getRuntimeMXBean().getName();
	    String PID = name.substring(0, name.indexOf("@"));	// Process ID of the program calling this function

	    //GC.class_histogram parameter forces JVM runs garbage collection
	    Process p = Runtime.getRuntime().exec("jcmd " + PID + " GC.class_histogram");
	    
	    //-histo:live modifier forces JVM runs garbage collection
	    //Process p = Runtime.getRuntime().exec("jmap " + "-histo:live " + PID);
	    
	    StringBuffer sb = new StringBuffer(1024*4);
	    BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream()));
	    
	    List<Object> lines = input.lines().collect(Collectors.toList());
	    
	    for(Object line : lines){
	    	if(((String) line).contains(package_name)) sb.append(line).append('\n');
	    }
	    sb.append(((String) lines.get(lines.size()-1)));
    	
    	return new String[]{(String) lines.get(2), sb.toString(), (String) lines.get(lines.size()-1)};
	}
	
	/**
	 * @return Return the precisely used memory in total. It forces to run GC
	 * @throws IOException
	 */
	public static double get_memory_sum() throws IOException {
	    String name = ManagementFactory.getRuntimeMXBean().getName();
	    String PID = name.substring(0, name.indexOf("@"));	// Process ID of the program calling this function

	    //GC.class_histogram parameter forces JVM runs garbage collection
	    Process p = Runtime.getRuntime().exec("jcmd " + PID + " GC.class_histogram");
	    
	    //-histo:live modifier forces JVM runs garbage collection
	    //Process p = Runtime.getRuntime().exec("jmap " + "-histo:live " + PID);
	    
	    BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream()));
	    List<Object> lines = input.lines().collect(Collectors.toList());
	    
	    String[] items = ((String) lines.get(lines.size()-1)).split(" ");
	    
    	return Double.parseDouble(items[items.length-1]);
	}
	
	/**
	 * Force to run GC. Run this function two times, 
	 * when a structure is alive and when a structure was destroyed
	 * to get precise memory amount occupied by the structure.
	 */
	public static void force_garbage_collection(){
	    String name = ManagementFactory.getRuntimeMXBean().getName();
	    String PID = name.substring(0, name.indexOf("@"));	// Process ID of the program calling this function

	    //GC.class_histogram parameter forces JVM runs garbage collection
	    //GC.class_histogram has higher impact compared to GC.run or GC.run_finalization
	    try {
			Runtime.getRuntime().exec("jcmd " + PID + " GC.class_histogram");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
