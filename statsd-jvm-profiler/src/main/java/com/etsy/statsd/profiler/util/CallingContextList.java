package com.etsy.statsd.profiler.util;

import java.util.*;
import java.lang.StackTraceElement;
import java.lang.management.ManagementFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Utility class for storing and updating function latencies for context-
 * specific functions according to stack traces using a linked list
 *
 * @author Zhi Xing
 */
public class CallingContextList {
	
    /**
     * ListElement for CallingContextList
	*/	
	public static class ListElement {
		public String name;
		public long latency;
		
		public ListElement(String s, long v) {
			name = s;
			latency = v;
		}
	}
	
	private LinkedList<ListElement> list = new LinkedList<ListElement>();
	private String data = "";
	
	private FSDataOutputStream out;
	
    /**
     * Constructor
	*/	
	public CallingContextList(FSDataOutputStream outStream) {
		out = outStream;
	}
	
    /**
     * Update the calling context list: (1) increase the latency of the stack-top function
     * (2) save all the disappeared functions to HDFS
     *
     * @param trace The stack trace elements
     * @param increment The amount by which the function latency increases
	*/
	public void update(StackTraceElement[] trace, long increment) {
		ListIterator<ListElement> it = list.listIterator();
		String context = "@";
		ListElement elem;
		for (int i = trace.length - 1; i >= 0; --i) {
			String name = String.format("%s.%s", trace[i].getClassName(), trace[i].getMethodName());
			
			if (!it.hasNext()) {
				elem = new ListElement(name, 1);
				it.add(elem);
			}
			else {
				elem = it.next(); 
				
				if (!elem.name.equals(name)) {
					it.previous();
					while (it.hasNext()) {
						elem = it.next();
						it.remove();
						context += "-" + elem.name;
						saveToHDFS(String.format("%s,%d\n", context, elem.latency));
					}
					
					elem = new ListElement(name, 1);
					it.add(elem);
				}
				else if (i == 0) {
					elem.latency += 1; 
				}
			}
			context += "-" + name;
		}
		
		while(it.hasNext()) {
			elem = it.next();
			it.remove();
			context += "-" + elem.name;
			saveToHDFS(String.format("%s,%d\n", context, elem.latency));
		}
	}
	
	
    /**
     * Flush the current saved stack trace
	*/
	public void flush() {
		ListIterator<ListElement> it = list.listIterator();
		String context = "@";
		ListElement elem;
		while (it.hasNext()) {
			elem = it.next();
			context += "-" + elem.name;
			saveToHDFS(String.format("%s,%d\n", context, elem.latency));
		}
		//System.out.println("@@@@@ saving data:\n" + data);
		//saveToHDFS();
		try {
			out.close();
		}
		catch (Exception e) {
    		System.out.println(e.toString());
    	}
	}
	
	/**
     * Helper to save data as string
     * 
     * @param str The string data to be saved
	*/
	private void save(String str) {
		data += str;
	}
	
	/**
     * Helper to save data into HDFS
	*/
	private void saveToHDFS(String str) {
		//System.out.println("@@@@@ Saving to file.");
		try {
			//FileSystem fs = FileSystem.get(new Configuration());			
            //FSDataOutputStream out = fs.append(file); 
			out.writeChars(str);
			//out.close();
		}
    	catch (Exception e) {
    		System.out.println(e.toString());
    	}
		//System.out.println("@@@@@ Saved to file.");
	}
}
