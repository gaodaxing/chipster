package fi.csc.microarray.gbrowser.index;

/*
 * Gene name search actions (creating database in memory, creating table,
 * updating it with gene information (chromosome, bp_start, bp_end, name),
 * getting gene coordinates by its name 
 */

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import fi.csc.microarray.client.visualisation.methods.gbrowser.GenomeBrowser;
import fi.csc.microarray.client.visualisation.methods.gbrowser.fileFormat.ColumnType;
import fi.csc.microarray.client.visualisation.methods.gbrowser.message.RegionContent;

public class GeneIndexActions {

	private Connection conn;
	private Statement st;
	private static GeneIndexActions gia;
	private GetGeneIndexData getData;
	
	private GeneIndexActions(GenomeBrowser genomeBrowser){
		try {
			getData = new GetGeneIndexData(genomeBrowser);
        	Class.forName("org.h2.Driver");
        	conn = DriverManager.getConnection("jdbc:h2:mem:GeneIndex", "sa", "");
        	createTable();
        	updateTable(getData.read());
        } catch (ClassNotFoundException e) {
        	// TODO Auto-generated catch block
        	e.printStackTrace();
        } catch (SQLException e) {
        	// TODO Auto-generated catch block
        	e.printStackTrace();
        }
	}
    
	/**
	 * creates database, table, updates table with gene indexes
	 */
	public static GeneIndexActions getInstance(GenomeBrowser genomeBrowser) {
		if (gia == null){
			gia = new GeneIndexActions(genomeBrowser);
		}
		return gia;
	}
	
	/*
	 * create table and index for geneName
	 */
    private void createTable(){
    	
    	try {
    		st = conn.createStatement();
			st.execute("CREATE TABLE gene_name_index(" +
					"ID INT PRIMARY KEY auto_increment," +
					"chromosome VARCHAR(2)," +
					"bp_start INT," +
					"bp_end INT," +
					"NAME VARCHAR(255),);" +
					"create index geneName on gene_name_index(name);");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
    private void updateTable(List<RegionContent> indexList){
    	try {
			st = conn.createStatement();
			for (RegionContent id : indexList) {
//				st.executeUpdate("insert into gene_name_index values (null," + 
//						id.values.get(ColumnType.CHROMOSOME) + "," + id.values.get(ColumnType.BP_START) + "," +
//						id.values.get(ColumnType.BP_END) + ",'" + id.values.get(ColumnType.DESCRIPTION) + "' )");
			}
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
    /*
     * getting location of a gene
     */
    public GeneIndexDataType getLocation(String name){
    	try {
			st = conn.createStatement();
			ResultSet rs = st.executeQuery("select chromosome,bp_start,bp_end from gene_name_index where name ='" + name + "'");
			rs.next();
			return new GeneIndexDataType(rs.getString(1), rs.getLong(2), rs.getLong(3));
			
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
    }
        
    public boolean checkIfNumber(String name){
    	try {
			Integer.parseInt(name);
			return true;
		} catch (NumberFormatException e) {
			try {
				Long.parseLong(name);
				return true;
			} catch (NumberFormatException e1) {
				return false;
			}
		}
    }
    
    public void closeConnection(){
    	try{
    		conn.close();
    	} catch (SQLException e) {
    		
    	}
    }
}
