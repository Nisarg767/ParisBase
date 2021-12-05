import java.io.RandomAccessFile;
import java.io.FileReader;
import java.io.File;
import java.util.Scanner;
import java.util.SortedMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Date;
import java.text.SimpleDateFormat;

public class UpdateTable {
	public static void parseUpdateString(String updtStr) {
		System.out.println("UPDATE METHOD");
		System.out.println("Parsing the string:\"" + updtStr + "\"");
		
		String[] tokens=updtStr.split(" ");
		String tbl = tokens[1];
		String[] tmp1 = updtStr.split("set");
		String[] tmp2 = tmp1[1].split("where");
		String cmpTemp = tmp2[1];
		String setTemp = tmp2[0];
		String[] comp = DavisBase.parserEquation(cmpTemp);
		String[] setLst = DavisBase.parserEquation(setTemp);
		if(!DavisBase.tableExists(tbl)){
			System.out.println("Table "+tbl+" does not exist.");
		}
		else
		{
			update(tbl, comp, setLst);
		}
		
	}
	public static void update(String tbl, String[] comp, String[] setLst){
		try{
			
			int keyVar = new Integer(comp[2]);
			
			RandomAccessFile tblFle = new RandomAccessFile("data/"+tbl+".tbl", "rw");
			int numPages = Table.pages(tblFle);
			int pageVar = 0;
			for(int pg = 1; pg <= numPages; pg++)
				if(Page.hasKey(tblFle, pg, keyVar)&Page.getPageType(tblFle, pg)==0x0D){
					pageVar = pg;
				}
			
			if(pageVar==0)
			{
				System.out.println("The key value that is given does not exist");
				return;
			}
			
			int[] kaysLst = Page.getKeyArray(tblFle, pageVar);
			int x = 0;
			for(int k = 0; k < kaysLst.length; k++)
				if(kaysLst[k] == keyVar)
					x = k;
			int cellOffset = Page.getCellOffset(tblFle, pageVar, x);
			long loca = Page.getCellLoc(tblFle, pageVar, x);
			
			String[] colsLst = Table.getColName(tbl);
			String[] valus = Table.retrieveValues(tblFle, loca);

			String[] dtaTyp = Table.getDataType(tbl);
			for(int k=0; k < dtaTyp.length; k++)
				if(dtaTyp[k].equals("DATE") || dtaTyp[k].equals("DATETIME"))
					valus[k] = "'"+valus[k]+"'";

			for(int k = 0; k < colsLst.length; k++)
				if(colsLst[k].equals(setLst[0]))
					x = k;
			valus[x] = setLst[2];

			String[] nullabl = Table.getNullable(tbl);
			for(int k = 0; k < nullabl.length; k++){
				if(valus[k].equals("null") && nullabl[k].equals("NO")){
					System.out.println("NULL-value constraint violation");
					return;
				}
			}
			byte[] stcVar = new byte[colsLst.length-1];
			int payLdSize = Table.calPayloadSize(tbl, valus, stcVar);
			Page.updateLeafCell(tblFle, pageVar, cellOffset, payLdSize, keyVar, stcVar, valus);

			tblFle.close();

		}catch(Exception ex){
			System.out.println(ex);
		}
	}

}