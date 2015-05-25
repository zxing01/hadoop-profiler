package com.etsy.statsd.profiler.profilers;

import com.etsy.statsd.profiler.Arguments;
import com.etsy.statsd.profiler.Profiler;
import com.etsy.statsd.profiler.reporter.Reporter;
import com.etsy.statsd.profiler.util.*;
import com.etsy.statsd.profiler.worker.ProfilerThreadFactory;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.util.*;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.FSDataOutputStream;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;

/**
 * Profiles context-specific CPU time spent in each method
 *
 * @author Zhi Xing
 */
public class CCTProfiler extends Profiler {
    private static final String PACKAGE_WHITELIST_ARG = "packageWhitelist";
    private static final String PACKAGE_BLACKLIST_ARG = "packageBlacklist";

    public static final long REPORTING_PERIOD = 10;
    public static final long PERIOD = 1;
    public static final List<String> EXCLUDE_PACKAGES = Arrays.asList("com.etsy.statsd.profiler", "com.timgroup.statsd");

    private StackTraceFilter filter;
    //private HashMap<Long, CallingContextTree> ccts = new HashMap<Long, CallingContextTree>();
    private HashMap<Long, CallingContextList> ccls = new HashMap<Long, CallingContextList>();
	private Random rand = new Random();
	private Configuration conf = new Configuration();
	private FileSystem fs;
    
    public CCTProfiler(Reporter reporter, Arguments arguments) {
        super(reporter, arguments);
		conf.setBoolean("fs.automatic.close", false);
		try {
			fs = FileSystem.get(conf);
		}
		catch (Exception e) {
    		System.out.println(e.toString());
    	}
    }

    /**
     * Profile CPU time by method call
     */
    @Override
    public void profile() {
        for (ThreadInfo thread : getAllRunnableThreads()) {
        	//System.out.println("@@@@@ current thread name: " + thread.getThreadName());
            // certain threads do not have stack traces
            if (thread.getStackTrace().length > 0) {
            	//System.out.println("@@@@@ has stack trace");
                String traceKey = StackTraceFormatter.formatStackTrace(thread.getStackTrace());
                if (filter.includeStackTrace(traceKey)) {
                	//System.out.println("@@@@@ passed the filter");
                	try {
                		CallingContextList ccl;
                		if (ccls.containsKey(thread.getThreadId())) {
                			ccl = ccls.get(thread.getThreadId());
                		}
                		else {
                			Path file = new Path(String.format("profiler/%s-%d", ManagementFactory.getRuntimeMXBean().getName(), rand.nextInt(Integer.MAX_VALUE)));
                			while (fs.exists(file)) {
                				file = new Path(String.format("profiler/%s-%d", ManagementFactory.getRuntimeMXBean().getName(), rand.nextInt(Integer.MAX_VALUE))); 
                			}
                			FSDataOutputStream out = fs.create(file);
                			ccl = new CallingContextList(out);
                			ccls.put(thread.getThreadId(), ccl);
                		}
                		ccl.update(thread.getStackTrace(), PERIOD);
                	}
                	catch (Exception e) {
                		System.out.println(e.toString());
                	}
                }
            }
        }
    }

    /**
     * Save data on shutdown
     */
    @Override
    public void flushData() {
    	//CallingContextTree cct = CallingContextTree.combine(ccts.values().toArray(new CallingContextTree[0]));
    	//System.out.println(CallingContextTree.toString(cct));
    	//CallingContextTree.serialize(cct, "/tmp/cct.txt");
    	//CallingContextTree ct = CallingContextTree.deserialize("/tmp/cct.txt");
    	//System.out.println("TEST ROOT: " + ct.getRoot().name);
    	//CallingContextTree.toString(ct);
    	//System.out.println("EQUAL TREE? " + Boolean.toString(CallingContextTree.dfs3(cct.getRoot(), ct.getRoot())));
    	    	
    	for (Long i : ccls.keySet().toArray(new Long[0])) {
    		ccls.get(i).flush();
    	}
    	try {
        	fs.close();
    	}
    	catch (Exception e) {
    		System.out.println(e.toString());
    	} 
    	/*
    	try {
    		Configuration conf = new Configuration();
    		Random rand = new Random();
        	FileSystem fs = FileSystem.get(conf);
        	
        	String output;
        	CallingContextTree cct;
        	Path file;
        	FSDataOutputStream out;
        	
        	System.out.println("@@@@@ number of trees: " + ccts.size());
        	//for (Long i : ccts.keySet().toArray(new Long[0])) {
        		//System.out.println("@@@@@ current key: " + i);
        		//cct = ccts.get(i);
        		cct = CallingContextTree.combine(ccts.values().toArray(new CallingContextTree[0]));
        		//output = CallingContextTree.toString(cct);
        		System.out.println("@@@@@ combined ccts.");
            	file = new Path("profiler/file-" + String.valueOf(rand.nextInt(Integer.MAX_VALUE)) + ".txt");
            	while (fs.exists(file)) {
            		file = new Path("profiler/file-" + String.valueOf(rand.nextInt(Integer.MAX_VALUE)) + ".txt"); 
            	}
            	System.out.println("@@@@@ opening file: " + file.getName());            	
            	out = fs.create(file);
            	//out.writeChars(output);
            	
            	CallingContextTree.saveToHDFS(cct, out);
            	
            	out.close();
            	System.out.println("@@@@@ closed file: " + file.getName());
        	//}
    	}
    	catch (Exception e) {
    		System.out.println(e.getMessage());
    	}
    	*/
    }

    @Override
    public long getPeriod() {
        return PERIOD;
    }

    @Override
    public TimeUnit getTimeUnit() {
        return TimeUnit.MILLISECONDS;
    }

    @Override
    protected void handleArguments(Arguments arguments) {
        List<String> packageWhitelist = parsePackageList(arguments.remainingArgs.get(PACKAGE_WHITELIST_ARG));
        List<String> packageBlacklist = parsePackageList(arguments.remainingArgs.get(PACKAGE_BLACKLIST_ARG));
        filter = new StackTraceFilter(packageWhitelist, Lists.newArrayList(Iterables.concat(EXCLUDE_PACKAGES, packageBlacklist)));
    }

    /**
     * Parses a colon-delimited list of packages
     *
     * @param packages A string containing a colon-delimited list of packages
     * @return A List of packages
     */
    private List<String> parsePackageList(String packages) {
        if (packages == null) {
            return new ArrayList<>();
        } else {
            return Arrays.asList(packages.split(":"));
        }
    }

    /**
     * Gets all runnable threads, excluding profiler threads
     *
     * @return A Collection<ThreadInfo> representing current thread state
     */
    private Collection<ThreadInfo> getAllRunnableThreads() {
        return ThreadDumper.filterAllThreadsInState(false, false, Thread.State.RUNNABLE, new Predicate<ThreadInfo>() {
            @Override
            public boolean apply(ThreadInfo input) {
                return !input.getThreadName().startsWith(ProfilerThreadFactory.NAME_PREFIX);
            }
        });
    }
}
