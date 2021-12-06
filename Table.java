import java.io.RandomAccessFile;
import java.io.FileReader;
import java.io.File;
import java.util.Scanner;
import java.util.SortedMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Date;
import java.text.SimpleDateFormat;

public class Table{
	
	public static int pageSize = 512;
	public static String datePattern = "yyyy-MM-dd_HH:mm:ss";

	public static void main(String[] args){}
	
	public static int pages(RandomAccessFile fle){
		int numberOfPages = 0;
		try{
			numberOfPages = (int)(fle.length()/(new Long(pageSize)));
		}catch(Exception ex){
			System.out.println(ex);
		}

		return numberOfPages;
	}

	public static String[] getColName(String tble){ //tables=davisbase_tables
		String[] cols = new String[0];
		try{
			RandomAccessFile tblFile = new RandomAccessFile("data/davisbase_columns.tbl", "rw");
			Buffer bufferObj = new Buffer();
			String[] columnNames = {"rowid", "table_name", "column_name", "data_type", "ordinal_position", "is_nullable"};
			String[] comp = {"table_name","=",tble};
			filter(tblFile, comp, columnNames, bufferObj);
			HashMap<Integer, String[]> content = bufferObj.content;
			ArrayList<String> arrList = new ArrayList<String>();
			for(String[] k : content.values()){
				arrList.add(k[2]);
			}
			int size=arrList.size();
			cols = arrList.toArray(new String[size]);
			tblFile.close();
			return cols;
		}catch(Exception ex){
			System.out.println(ex);
		}
		return cols;
	}
	
	public static String[] getDataType(String tbl){
		String[] dataTyp = new String[0];
		try{
			RandomAccessFile tblFile = new RandomAccessFile("data/davisbase_columns.tbl", "rw");
			Buffer bufferObj = new Buffer();
			String[] columnNames = {"rowid", "table_name", "column_name", "data_type", "ordinal_position", "is_nullable"};
			String[] comp = {"table_name","=",tbl};
			filter(tblFile, comp, columnNames, bufferObj);
			HashMap<Integer, String[]> content = bufferObj.content;
			ArrayList<String> arrList = new ArrayList<String>();
			for(String[] x : content.values()){
				arrList.add(x[3]);
			}
			int size=arrList.size();
			dataTyp = arrList.toArray(new String[size]);
			tblFile.close();
			return dataTyp;
		}catch(Exception ex){
			System.out.println(ex);
		}
		return dataTyp;
	}

	
	public static void filter(RandomAccessFile tblFile, String[] comp, String[] columnNames, Buffer bufferObj){
		try{
			int numOfPages = pages(tblFile);
			for(int eachPage = 1; eachPage <= numOfPages; eachPage++){
				
				tblFile.seek((eachPage-1)*pageSize);
				byte pageTyp = tblFile.readByte();
				if(pageTyp == 0x0D)
				{
					byte numOfCells = Page.getCellNumber(tblFile, eachPage); //accesses tblFile header to get number of cells.

					for(int k=0; k < numOfCells; k++){
						
						long loca = Page.getCellLoc(tblFile, eachPage, k);
						String[] valus = retrieveValues(tblFile, loca);
						int rowId=Integer.parseInt(valus[0]);

						boolean isCheck = cmpCheck(valus, rowId, comp, columnNames);
						
						if(isCheck)
							bufferObj.add_vals(rowId, valus);
					}
				}
				else
					continue;
			}

			bufferObj.columnName = columnNames;
			bufferObj.format = new int[columnNames.length];

		}catch(Exception ex){
			System.out.println("Error");
			ex.printStackTrace();
		}

	}

		public static String[] retrieveValues(RandomAccessFile tblFile, long loca){
		
		String[] valusLst = null;
		try{
			
			SimpleDateFormat dateFormt = new SimpleDateFormat (datePattern);

			tblFile.seek(loca+2);
			int key = tblFile.readInt();
			int num_cols = tblFile.readByte();
			
			byte[] stcVar = new byte[num_cols];
			tblFile.read(stcVar);
			
			valusLst = new String[num_cols+1];
			
			valusLst[0] = Integer.toString(key);
			
			for(int k=1; k <= num_cols; k++){
				switch(stcVar[k-1]){
					case 0x00:  tblFile.readByte();
					            valusLst[k] = "null";
								break;

					case 0x01:  tblFile.readShort();
					            valusLst[k] = "null";
								break;

					case 0x02:  tblFile.readInt();
					            valusLst[k] = "null";
								break;

					case 0x03:  tblFile.readLong();
					            valusLst[k] = "null";
								break;

					case 0x04:  valusLst[k] = Integer.toString(tblFile.readByte());
								break;

					case 0x05:  valusLst[k] = Integer.toString(tblFile.readShort());
								break;

					case 0x06:  valusLst[k] = Integer.toString(tblFile.readInt());
								break;

					case 0x07:  valusLst[k] = Long.toString(tblFile.readLong());
								break;

					case 0x08:  valusLst[k] = String.valueOf(tblFile.readFloat());
								break;

					case 0x09:  valusLst[k] = String.valueOf(tblFile.readDouble());
								break;

					case 0x0A:  Long tmp = tblFile.readLong();
								Date dateTime = new Date(tmp);
								valusLst[k] = dateFormt.format(dateTime);
								break;

					case 0x0B:  tmp = tblFile.readLong();
								Date date = new Date(tmp);
								valusLst[k] = dateFormt.format(date).substring(0,10);
								break;

					default:    int len = new Integer(stcVar[k-1]-0x0C);
								byte[] bytes = new byte[len];
								tblFile.read(bytes);
								valusLst[k] = new String(bytes);
								break;
				}
			}

		}catch(Exception ex){
			System.out.println(ex);
		}

		return valusLst;
	}
		
	public static int calPayloadSize(String tbl, String[] valsLst, byte[] stcVar){
		String[] dataTyp = getDataType(tbl);
		int size =dataTyp.length;
		for(int k = 1; k < dataTyp.length; k++){
			stcVar[k - 1]= getStc(valsLst[k], dataTyp[k]);
			size = size + feildLength(stcVar[k - 1]);
		}
		return size;
	}
	
	public static byte getStc(String value, String dataTyp){
		if(value.equals("null")){
			switch(dataTyp){
				case "TINYINT":     return 0x00;
				case "INT":			return 0x02;
				case "SMALLINT":    return 0x01;
				case "REAL":        return 0x02;
				case "DOUBLE":      return 0x03;
				case "BIGINT":      return 0x03;
				case "DATETIME":    return 0x03;
				case "TEXT":        return 0x03;
				case "DATE":        return 0x03;
				default:			return 0x00;
			}							
		}else{
			switch(dataTyp){
				case "TINYINT":     return 0x04;
				case "INT":			return 0x06;
				case "REAL":        return 0x08;
				case "BIGINT":      return 0x07;
				case "DOUBLE":      return 0x09;
				case "SMALLINT":    return 0x05;
				case "DATE":        return 0x0B;
				case "DATETIME":    return 0x0A;
				case "TEXT":        return (byte)(value.length()+0x0C);
				default:			return 0x00;
			}
		}
	}
	
    public static short feildLength(byte stcVar){
		switch(stcVar){
			case 0x00: return 1;
			case 0x04: return 1;
			case 0x01: return 2;
			case 0x05: return 2;
			case 0x02: return 4;
			case 0x08: return 4;
			case 0x06: return 4;
			case 0x03: return 8;
			case 0x07: return 8;
			case 0x09: return 8;
			case 0x0A: return 8;
			case 0x0B: return 8;
			default:   return (short)(stcVar - 0x0C);
		}
	}


	
public static int searchKeyPage(RandomAccessFile tblFile, int key){
		int val = 1;
		try{
			int numberOfPages = pages(tblFile);
			for(int eachPage = 1; eachPage <= numberOfPages; eachPage++){
				tblFile.seek((eachPage - 1)*pageSize);
				byte pageTyp = tblFile.readByte();
				if(pageTyp == 0x0D){
					int[] keys = Page.getKeyArray(tblFile, eachPage);
					if(keys.length == 0)
						return 0;
					int rm = Page.getRightMost(tblFile, eachPage);
					if(keys[0] <= key && key <= keys[keys.length - 1]){
						return eachPage;
					}else if(rm == 0 && keys[keys.length - 1] < key){
						return eachPage;
					}
				}
			}
		}catch(Exception ex){
			System.out.println(ex);
		}

		return val;
	}

	
	public static String[] getNullable(String tbl){
		String[] nullable = new String[0];
		try{
			RandomAccessFile tblFile = new RandomAccessFile("data/davisbase_columns.tbl", "rw");
			Buffer bufferObj = new Buffer();
			String[] columnNames = {"rowid", "table_name", "column_name", "data_type", "ordinal_position", "is_nullable"};
			String[] comp = {"table_name","=",tbl};
			filter(tblFile, comp, columnNames, bufferObj);
			HashMap<Integer, String[]> content = bufferObj.content;
			ArrayList<String> arrList = new ArrayList<String>();
			for(String[] k : content.values()){
				arrList.add(k[5]);
			}
			int size=arrList.size();
			nullable = arrList.toArray(new String[size]);
			tblFile.close();
			return nullable;
		}catch(Exception ex){
			System.out.println(ex);
		}
		return nullable;
	}


	public static void filter(RandomAccessFile tblFile, String[] comp, String[] columnNames, String[] typ, Buffer bufferObj){
		try{
			
			int numOfPages = pages(tblFile);
			
			for(int eachPage = 1; eachPage <= numOfPages; eachPage++){
				
				tblFile.seek((eachPage-1)*pageSize);
				byte pageTyp = tblFile.readByte();
				
					if(pageTyp == 0x0D){
						
					byte numOfCells = Page.getCellNumber(tblFile, eachPage);

					 for(int k=0; k < numOfCells; k++){
						long loca = Page.getCellLoc(tblFile, eachPage, k);
						String[] valsLst = retrieveValues(tblFile, loca);
						int roId=Integer.parseInt(valsLst[0]);
						
						for(int l=0; l < typ.length; l++)
							if(typ[l].equals("DATE") || typ[l].equals("DATETIME"))
								valsLst[l] = "'"+valsLst[l]+"'";
						
						boolean check = cmpCheck(valsLst, roId , comp, columnNames);

						
						for(int l=0; l < typ.length; l++)
							if(typ[l].equals("DATE") || typ[l].equals("DATETIME"))
								valsLst[l] = valsLst[l].substring(1, valsLst[l].length()-1);

						if(check)
							bufferObj.add_vals(roId, valsLst);
					 }
				   }
				    else
						continue;
			}

			bufferObj.columnName = columnNames;
			bufferObj.format = new int[columnNames.length];

		}catch(Exception ex){
			System.out.println("Error at filter");
			ex.printStackTrace();
		}

	}

	
	public static boolean cmpCheck(String[] valusLst, int roId, String[] comp, String[] columnNames){

		boolean isCheck = false;
		
		if(comp.length == 0){
			isCheck = true;
		}
		else{
			int columPos = 1;
			for(int k = 0; k < columnNames.length; k++){
				if(columnNames[k].equals(comp[0])){
					columPos = k + 1;
					break;
				}
			}
			
			if(columPos == 1){
				int val = Integer.parseInt(comp[2]);
				String operator = comp[1];
				switch(operator){
					case "=": if(roId == val)
								isCheck = true;
							  else
							  	isCheck = false;
							  break;
					case ">": if(roId > val)
								isCheck = true;
							  else
							  	isCheck = false;
							  break;
					case ">=": if(roId >= val)
						        isCheck = true;
					          else
					  	        isCheck = false;
					          break;
					case "<": if(roId < val)
								isCheck = true;
							  else
							  	isCheck = false;
							  break;
					case "<=": if(roId <= val)
								isCheck = true;
							  else
							  	isCheck = false;
							  break;
					case "!=": if(roId != val)
								isCheck = true;
							  else
							  	isCheck = false;
							  break;						  							  							  							
				}
			}else{
				if(comp[2].equals(valusLst[columPos-1]))
					isCheck = true;
				else
					isCheck = false;
			}
		}
		return isCheck;
	}
	
}
