package com.etsy.statsd.profiler.util;

import java.util.*;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.lang.StackTraceElement;

import org.apache.hadoop.fs.FSDataOutputStream;

/**
 * Utility class for storing and updating function latencies for context-
 * specific functions according to stack traces using a n-ary tree
 *
 * @author Zhi Xing
 */
public class CallingContextTree {
	
    /**
     * TreeNode for CallingContextTree
	*/	
	public static class TreeNode {
		public String name;
		public long latency;
		public boolean active;
		public LinkedList<TreeNode> children;
		
		public TreeNode(String s, long v) {
			name = s;
			latency = v;
			active = true;
			children = new LinkedList<TreeNode>();
		}
	}
	
	private TreeNode root;
	
    /**
     * Constructor
	*/	
	public CallingContextTree(String name) {
		root = new TreeNode(name, 0);
	}

    /**
     * Helper function to recursively mark nodes as inactive
     *
     * @param node The node to be marked as inactive
	*/
	private void markInactive(TreeNode node) {
		if (node != null) {
			node.active = false;
			if (node.children.size() > 0) {
				markInactive(node.children.getLast());
			}
		}
	}
	
    /**
     * Update the calling context tree: (1) increase the latency of the stack-top function
     * (2) marking all the functions that were on the stack but not now as inactive
     *
     * @param trace The stack trace elements
     * @param increment The amount by which the function latency increases
	*/
	public void update(StackTraceElement[] trace, long increment) {
		System.out.println("@@@@@ size = " + trace.length);
		TreeNode cur = root;
		
		for (int i = trace.length - 1; i >= 0; --i) {
			String name = String.format("%s.%s", trace[i].getClassName(), trace[i].getMethodName());
			//System.out.print("@@@@@ parent name = " + cur.name + " name = " + name + ": ");
			
			if (cur.children.size() == 0 
					|| !cur.children.getLast().name.equals(name)
					|| !cur.children.getLast().active) {
				/*
				if (cur.children.size() == 0) {
					System.out.println("1");
				}
				else if (!cur.children.getLast().name.equals(name)) {
					System.out.println("2");
				}
				else if (!cur.children.getLast().active) {
					System.out.println("3");
				}
				*/
				TreeNode node = new TreeNode(name, increment);
				cur.children.addLast(node);
				cur = node;
			}
			else {
				//System.out.println("4");
				TreeNode node = cur.children.getLast();
				cur = node;
				if (i == 0) {
					node.latency += increment;
				}
			}
		}
		
		if (cur.children.size() > 0) {
			markInactive(cur.children.getLast());
		}
	}
	
/*	private interface NodeOperator
	{
		public void preopt(TreeNode tn);
		public void postopt(TreeNode tn);
	}
	
	public static void dfs(TreeNode tn, NodeOperator opt)
	{
		if(tn == null)
			return;
		opt.preopt(tn);
		for(TreeNode ntn : tn.children){
			dfs(ntn, opt);
		}
		opt.postopt(tn);
	}*/
	
	public static void dfs1(TreeNode tn, int tabs)
	{
		if(tn == null)
			return;
		String tbs = "";
		for(int i = 0; i < tabs; ++i)
			tbs += " ";
		System.out.println(tbs + tn.name + "-" + String.format("%d",tn.latency));
		for(TreeNode ntn : tn.children){
			dfs1(ntn, tabs + 1);
		}
	}
	
	public static boolean dfs3(TreeNode tn1, TreeNode tn2)
	{
		if((tn1 == null) ^ (tn2 == null)){
			System.out.println("not same status");
			return false;
		}
		if(tn1 == null)
			return true;
		if(!tn1.name.equals(tn2.name) || tn1.latency != tn2.latency){
			System.out.println("Node not match: " + tn1.name + " " + tn2.name + " " + Long.toString(tn1.latency) + " " + Long.toString(tn2.latency));
			return false;
		}
		Iterator<TreeNode> it1 = tn1.children.iterator();
		Iterator<TreeNode> it2 = tn2.children.iterator();
		while(it1.hasNext() || it2.hasNext()){
			if(it1.hasNext() != it2.hasNext() || !dfs3(it1.next(),it2.next()))
				return false;
		}
		
		return true;
	}
	
	public static void dfs4(TreeNode tn, String path, List<String> kv){
		if(tn == null)
			return;
		if(!path.equals(""))
			path += "-" + tn.name;
		else
			path = tn.name;
		//System.out.println(tn.name);
		if (tn.latency > 1) 
			kv.add(path + "," + String.format("%d",tn.latency));
		for(TreeNode ntn : tn.children){
			dfs4(ntn, path, kv);
		}
	}

	
	public static String toString(CallingContextTree tree) {
		if(tree == null || tree.root == null)
			return null;
		
		/*final class Sopt implements NodeOperator{
			public String postr;
			private int tabs;
			public Sopt(){
				postr = new String();
				tabs = 0;
			}
			@Override
			public void preopt(TreeNode tn) {
				tabs 
				postr += tn.name + '-' + tn.latency + '.';
			}
			@Override
			public void postopt(TreeNode tn) {}
		}
		Sopt rst = new Sopt();
		dfs(tree.root, rst);
		
		//return rst.postr.substring(0, rst.postr.length() - 1);
		return rst.postr;*/
		
		//dfs1(tree.root, 0);
		List<String> kv = new ArrayList<String>();
		dfs4(tree.root, "", kv);
		String output = "";
		for(String flt : kv)
			output += flt +"\n";
		//return rst.postr.substring(0, rst.postr.length() - 1);
		return output;		
		
		
	}
	
	public static void dfs5(TreeNode tn, String path, FSDataOutputStream kv) throws IOException{
		if(tn == null)
			return;
		
		if (tn.name.charAt(0) != '@') {
			if(!path.equals(""))
				path += "-" + tn.name;
			else
				path = tn.name;
			//if(tn.latency > 1)
				kv.writeChars(path + "," + String.format("%d\n",tn.latency));
		}
		for(TreeNode ntn : tn.children){
			dfs5(ntn, path, kv);
		}
	}

	
	
	public static void saveToHDFS(CallingContextTree tree, FSDataOutputStream out) throws IOException {
		System.out.println("@@@@@ In saveToHDFS.");
		if(tree == null || tree.root == null || out == null)
			return;
		System.out.println("@@@@@ Starting dfs5.");
		dfs5(tree.root, "", out);
		//return rst.postr.substring(0, rst.postr.length() - 1);	
	}	
	
	
	
	public TreeNode getRoot()
	{
		return root;
	}
	
	
	public static void dfs2(TreeNode tn, PrintWriter pw)
	{
		if(tn == null)
			return;
		pw.write(tn.name + "-" + tn.latency + "\n");
		for(TreeNode ntn : tn.children){
			dfs2(ntn, pw);
		}
		pw.write(")\n");
	}
	
	public static void serialize(CallingContextTree tree, String filename) {
		if(tree == null || tree.root == null)
			return;
		
		/*final class Sopt implements NodeOperator{
			public PrintWriter pw;
			public Sopt(PrintWriter w){
				pw = w;
			}
			@Override
			public void preopt(TreeNode tn) {
				pw.write(tn.name + "-" + tn.latency + ".");
			}
			@Override
			public void postopt(TreeNode tn) {
				pw.write(").");
			}
		}
		try {
			PrintWriter writer = new PrintWriter(filename, "UTF-8");
			Sopt rst = new Sopt(writer);
			dfs2(tree.root, rst);
			writer.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
		
		try {
			PrintWriter writer = new PrintWriter(filename, "UTF-8");
			dfs2(tree.root, writer);
			writer.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
				
	}
	
/*	public static CallingContextTree deserialize(String filename) {
		try{
			BufferedReader br = new BufferedReader(new FileReader(filename));

			StringBuilder sb = new StringBuilder();
			String line = br.readLine();
			while (line != null) {
				sb.append(line);
				sb.append(System.lineSeparator());
		        line = br.readLine();
		    }
			String everything = sb.toString();
			String[] snodes = everything.split("\n");
			CallingContextTree cct = new CallingContextTree(snodes[0].split("-")[0]);
			int[] index = {-1};
			buildContextTree(cct.root, snodes, index);
			br.close();
			
		 } catch (Exception e){
			 e.printStackTrace();
			  
		 }
		 return null;
		 
	}*/
	
	
	public static CallingContextTree deserialize(String filename) {
		try{
			BufferedReader br = new BufferedReader(new FileReader(filename));

			//StringBuilder sb = new StringBuilder();
			//String line = br.readLine();
			//if(line == null)
				//return null;
			//while (line != null) {
			//	sb.append(line);
			//	sb.append(System.lineSeparator());
		    //    line = br.readLine();
		    //}
			//String everything = sb.toString();
			//String[] snodes = everything.split("\n");
			//CallingContextTree cct = new CallingContextTree(line.split("-")[0]);
			//cct.root.latency = Long.parseLong(line.split("-")[1]);
			CallingContextTree cct = new CallingContextTree(null);
			buildContextTree(cct.root, br);
			//System.out.println(Boolean.toString(cct.root == null));
			br.close();
			return cct;
			
		 } catch (Exception e){
			 e.printStackTrace();
			  
		 }
		 return null;
		 
	}
	
	private static boolean buildContextTree(TreeNode tn, BufferedReader br)
	{
		
		
		try {
			String line = br.readLine();
			//System.out.println("bf tree: " + line);
			if(line == null || line.equals(")"))
				return true;
			String[] vpair = line.split("-");
			//tn = new TreeNode(vpair[0], Long.parseLong(vpair[1]));
			tn.name = vpair[0];
			tn.latency = Long.parseLong(vpair[1]);
			for(;;){
				TreeNode ctn = new TreeNode("",0);
				if(buildContextTree(ctn,br))
					break;
				tn.children.addLast(ctn);
			}
			//System.out.println("af tree: " + tn.name + "-" + Long.toString(tn.latency));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return false;
	}
	
/*	private static boolean buildContextTree(TreeNode tn, String[] snodes, int[] index)
	{
		index[0]++;
		if(index[0] >= snodes.length || snodes[index[0]].equals(")"))
			return true;
		String[] vpair = snodes[index[0]].split("-");
		tn = new TreeNode(vpair[0], Long.parseLong(vpair[1]));
		while(index[0] < snodes.length - 1){
			tn.children.addLast(null);
			if(buildContextTree(tn.children.getLast(),snodes,index))
				break;
		}
		
		return false;
	}*/
	
	public static CallingContextTree combine(CallingContextTree[] ccts) {
		CallingContextTree cct = new CallingContextTree("@root");
		for (int i = 0; i < ccts.length; ++i) {
			cct.root.children.add(ccts[i].root);
		}
		
		return cct;
	}
}
