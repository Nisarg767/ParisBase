import java.io.RandomAccessFile;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Scanner;
import java.util.SortedMap;
public class DeleteTable {
	public static void parseDeleteString(String delStr) {
		System.out.println("DELETE OPERATION");
		System.out.println("Parsing the string:\"" + delStr + "\"");
		
		String[] valTkn=delStr.split(" ");
		String tbl = valTkn[3];
		String[] dumy = delStr.split("where");
		String cmpTemp = dumy[1];
		String[] comp = DavisBase.parserEquation(cmpTemp);
		if(!DavisBase.tableExists(tbl)){
			System.out.println("Table "+tbl+" does not exist.");
		}
		else
		{
			delete(tbl, comp);
		}
	}
	public static void delete(String tbl, String[] comp){
		try{
		int key = new Integer(comp[2]);

		RandomAccessFile fle = new RandomAccessFile("data/"+tbl+".tbl", "rw");
		int numPages = Table.pages(fle);
		int eachPag = 0;
		for(int pg = 1; pg <= numPages; pg++)
			if(Page.hasKey(fle, pg, key)&Page.getPageType(fle, pg)==0x0D){
				eachPag = pg;
				break;
			}
		
		if(eachPag==0)
		{
			System.out.println("The given key value does not exist");
			return;
		}
		
		short[] addrOfCell = Page.getCellArray(fle, eachPag);
		int k = 0;
		for(int l = 0; l < addrOfCell.length; l++)
		{
			long loc = Page.getCellLoc(fle, eachPag, l);
			String[] vals = Table.retrieveValues(fle, loc);
			int d = new Integer(vals[0]);
			if(d!=key)
			{
				Page.setCellOffset(fle, eachPag, k, addrOfCell[l]);
				k++;
			}
		}
		Page.setCellNumber(fle, eachPag, (byte)k);
		
		}catch(Exception e)
		{
			System.out.println(e);
		}
		
	}

}
