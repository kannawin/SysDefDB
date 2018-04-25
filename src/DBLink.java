import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import com.datastax.driver.core.*;


public class DBLink {
	private static Cluster cluster;
	private static Session session;
	private static String rootLabel = "vcesystem";
	
	public static void main(String[] args)
	{
		String server = "127.0.0.1";
		String keyspace = "nodes";
		
		cluster = Cluster.builder()
				.addContactPoint(server).build();
		
		session = cluster.connect();
		for(int z = 0; z < 25;z++) {
			session.execute("DROP KEYSPACE IF EXISTS nodes");
			session.execute("CREATE KEYSPACE nodes WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
			session.execute("CREATE TABLE nodes.tables (id varchar PRIMARY KEY, tables varchar);");
			ArrayList<Long> timers = new ArrayList<Long>();
			
			//TODO BUILD TABLES HERE
			timers.add(System.nanoTime());
			buildTables("/home/dna/Desktop/Graph/DataDump/nodes");
			timers.add(System.nanoTime());
			timers.add(System.nanoTime());
			buildRelationTable("/home/dna/Desktop/Graph/DataDump/edges.csv");
			timers.add(System.nanoTime());
			
			/*
			String startNode = "29ff96320eb348e48f7293e96bf8458e";		//PSU
			String endNode = "48453f8815c94df1a96753c326ad500d";		//Storage Disk
			//pointToPoint(startNode, endNode);
	
			ResultSet rs = session.execute("SELECT Source FROM nodes.edges WHERE Target = 'V76FN0516001' ALLOW FILTERING;");
			for(Row row : rs) {
				System.out.println(row.getString("Source"));
			}
			*/
			printTimers(timers);
		}
		
		session.close();
		cluster.close();
	}
	
	public static void pointToPoint(String id1, String id2) {
		ResultSet rs1 = session.execute("SELECT Source FROM nodes.edges WHERE Target = '" + id1 + "' ALLOW FILTERING;");
		ResultSet rs2 = session.execute("SELECT Source FROM nodes.edges WHERE Target = '" + id2 + "' ALLOW FILTERING;");
		ArrayList<String> ids = new ArrayList<String>();
		ids.add(id1);
		for(Row row : rs1) {
			ids.add(row.getString(0));
			ids = pointHelper(ids);
		}
		ids.add(id2);
		for(Row row: rs2) {
			ids.add(row.getString(0));
			ids = pointHelper(ids);
		}
		for(String s : ids) {
			System.out.println(s);
		}
	}
	
	public static ArrayList<String> pointHelper(ArrayList<String> ids){
		ResultSet rs = session.execute("SELECT Source FROM nodes.edges WHERE Target = '" + ids.get(ids.size() - 1) + "' ALLOW FILTERING;");
		if(rs.all().size() > 0) {
			for(Row row : rs) {
				ids.add(row.getString(0));
				pointHelper(ids);
			}
		}
		else {
			return ids;
		}
		return ids;
	}
	
	public static void printTimers(ArrayList<Long> timers)
	{
		for(int i = 0; i < timers.size(); i += 2) {
			long netTime = timers.get(i+1) - timers.get(i);
			double inSeconds = (double)netTime/1000000000.0;
			System.out.println("Test " + (i/2) + ":\t" + inSeconds + " seconds");
		}
	}
	
	
	public static void buildTables(String directory) {
		File file = new File(directory);
		File[] lof = file.listFiles();
		
		for(File f : lof) {
			file = new File(f.getPath());
			try {
				FileReader in = new FileReader(file);
				BufferedReader br = new BufferedReader(in);
				String nextLine = br.readLine();
				
				String[] attributes = nextLine.split(",");
				ArrayList<String> butes = new ArrayList<String>();
				ArrayList<String> columns = new ArrayList<String>();
				
				int primary = 0;
				
				for(int i = 0; i < attributes.length; i++) {
					columns.add(attributes[i]);
					if(!attributes[i].equals("Id")) {
						butes.add(attributes[i] + " varchar");
					}else {
						primary = i;
						butes.add(attributes[i] + " varchar PRIMARY KEY");
					}
				}
				String table = f.getName().substring(0, f.getName().length() - 4);

				buildTable(table, butes);
				
				while((nextLine = br.readLine()) != null) {
					ArrayList<String> stuff = new ArrayList<String>();
					String[] tempLine = nextLine.split(",");
					
					for(String s : tempLine) {
						stuff.add(s);
					}
					session.execute("INSERT INTO nodes.tables (id, tables) VALUES ('" + stuff.get(primary) + "','" + table + "');");
					insertInto(table,columns,stuff);
				}
				
			}catch(IOException e) {}
		}
	}
	
	public static void insertInto(String table, ArrayList<String> columns, ArrayList<String> stuff) {
		String insertText = "'";
		String attrText = "";
		
		boolean timeset = false;
		
		for(String s : columns) {
			attrText += s + ", ";
			if(s.equals("timeset")) {
				timeset = true;
			}
		}
		for(String s : stuff) {
			insertText += s + "', '";
		}
		attrText = attrText.substring(0, attrText.length() - 2);
		if(timeset) {
			attrText = attrText.substring(0,attrText.lastIndexOf(","));
		}
		insertText = insertText.substring(0, insertText.length() - 3);
		String query = "INSERT INTO nodes." + table + " (" + attrText + ") VALUES (" + insertText + ");";
		session.execute(query);
	}
	
	public static void buildTable(String table, ArrayList<String> columns) {
		String column = "";
		for(String s : columns) {
			column += s + ", ";
		}
		column = column.substring(0,column.length() - 2);
		String query = "CREATE TABLE nodes." + table + " (" + column + ");";
		session.execute(query);
	}
	
	public static void buildRelationTable(String relations) {
		File file = new File(relations);
		session.execute("CREATE TABLE nodes.edges (Source varchar, Target varchar, Id varchar PRIMARY KEY);");
		//session.execute("CREATE TABLE nodes.edges (Source varchar, Target varchar, Id varchar PRIMARY KEY, Type varchar);");
		
		try {
			FileReader in = new FileReader(file);
			BufferedReader br = new BufferedReader(in);
			String nextLine = br.readLine();
			
			while((nextLine = br.readLine()) != null) {
				String[] temp = nextLine.split(",");
				
				//String insertLine0 = "'" + temp[0] + "','" + temp[1] + "','" + temp[3] + "','contains'";
				//String insertLine1 = "'" + temp[0] + "','" + temp[1] + "','" + temp[3] + "','consumes'";
				String insertLine = "'" + temp[0] + "','" + temp[1] + "','" + temp[3] + "'";
				session.execute("INSERT INTO nodes.edges (Source, Target, Id) VALUES (" + insertLine + ");");
				
				//session.execute("INSERT INTO nodes.edges (Source, Target, Id, Type) VALUES (" + insertLine0 + ");");
				//session.execute("INSERT INTO nodes.edges (Source, Target, Id, Type) VALUES (" + insertLine1 + ");");
			}
		}catch(FileNotFoundException e) {} catch (IOException e) {}
	}
	
}
