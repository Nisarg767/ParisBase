import java.io.RandomAccessFile;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Scanner;
import java.util.SortedMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Arrays;
import java.util.Date;
import java.text.SimpleDateFormat;

public class Page{
	public static int pageSize = 512;
	public static final String datePattern = "yyyy-MM-dd_HH:mm:ss";

public static short calPayloadSize(String[] vals, String[] dtaTyp){
		int vlu = dtaTyp.length; 
		for(int k = 1; k < dtaTyp.length; k++){
			String daTy = dtaTyp[k];
			switch(daTy){
				case "TINYINT":
					vlu = vlu + 1;
					break;
				case "SMALLINT":
					vlu = vlu + 2;
					break;
				case "INT":
					vlu = vlu + 4;
					break;
				case "BIGINT":
					vlu = vlu + 8;
					break;
				case "REAL":
					vlu = vlu + 4;
					break;		
				case "DOUBLE":
					vlu = vlu + 8;
					break;
				case "DATETIME":
					vlu = vlu + 8;
					break;
				case "DATE":
					vlu = vlu + 8;
					break;
				case "TEXT":
					String text = vals[k];
					int len = text.length();
					vlu = vlu + len;
					break;
				default:
					break;
			}
		}
		return (short)vlu;
	}

	public static int makeInteriorPage(RandomAccessFile file){
		int num_pages = 0;
		try{
			num_pages = (int)(file.length()/(new Long(pageSize)));
			num_pages = num_pages + 1;
			file.setLength(pageSize * num_pages);
			file.seek((num_pages-1)*pageSize);
			file.writeByte(0x05); 
		}catch(Exception ex){
			System.out.println(ex);
		}

		return num_pages;
	}

	public static int makeLeafPage(RandomAccessFile file){
		int num_pages = 0;
		try{
			num_pages = (int)(file.length()/(new Long(pageSize)));
			num_pages = num_pages + 1;
			file.setLength(pageSize * num_pages);
			file.seek((num_pages-1)*pageSize);
			file.writeByte(0x0D); 
		}catch(Exception ex){
			System.out.println(ex);
		}

		return num_pages;

	}

	public static int findMidKey(RandomAccessFile file, int datPge){
		int vlu = 0;
		try{
			file.seek((datPge-1)*pageSize);
			byte pageType = file.readByte();
			int numCells = getCellNumber(file, datPge);
			int mid = (int) Math.ceil((double) numCells / 2);
			long loc = getCellLoc(file, datPge, mid-1);
			file.seek(loc);

			switch(pageType){
				case 0x05:
					file.readInt(); 
					vlu = file.readInt();
					break;
				case 0x0D:
					file.readShort();
					vlu = file.readInt();
					break;
			}

		}catch(Exception ex){
			System.out.println(ex);
		}

		return vlu;
	}

	
	public static void splitLeafPage(RandomAccessFile file, int curPage, int newPage){
		try{
			
			int numCells = getCellNumber(file, curPage);
			
			int mid = (int) Math.ceil((double) numCells / 2);

			int numCellA = mid - 1;
			int numCellB = numCells - numCellA;
			int content = 512;

			for(int k = numCellA; k < numCells; k++){
				long loc = getCellLoc(file, curPage, k);
				file.seek(loc);
				int cellSize = file.readShort()+6;
				content = content - cellSize;
				file.seek(loc);
				byte[] cell = new byte[cellSize];
				file.read(cell);
				file.seek((newPage-1)*pageSize+content);
				file.write(cell);
				setCellOffset(file, newPage, k - numCellA, content);
			}

			
			file.seek((newPage-1)*pageSize+2);
			file.writeShort(content);

			
			short enumOfSet = getCellOffset(file, curPage, numCellA-1);
			file.seek((curPage-1)*pageSize+2);
			file.writeShort(enumOfSet);

			
			int rightMost = getRightMost(file, curPage);
			setRightMost(file, newPage, rightMost);
			setRightMost(file, curPage, newPage);

			
			int parent = getParent(file, curPage);
			setParent(file, newPage, parent);

			
			byte varNum = (byte) numCellA;
			setCellNumber(file, curPage, varNum);
			varNum = (byte) numCellB;
			setCellNumber(file, newPage, varNum);
			
		}catch(Exception ex){
			System.out.println(ex);
			
		}
	}
	
	public static void splitInteriorPage(RandomAccessFile file, int curPage, int newPage){
		try{
			
			int numCells = getCellNumber(file, curPage);
			
			int mid = (int) Math.ceil((double) numCells / 2);

			int numCellA = mid - 1;
			int numCellB = numCells - numCellA - 1;
			short content = 512;

			for(int k = numCellA+1; k < numCells; k++){
				long loc = getCellLoc(file, curPage, k);
				short cellSize = 8;
				content = (short)(content - cellSize);
				file.seek(loc);
				byte[] cell = new byte[cellSize];
				file.read(cell);
				file.seek((newPage-1)*pageSize+content);
				file.write(cell);
				file.seek(loc);
				int datPge = file.readInt();
				setParent(file, datPge, newPage);
				setCellOffset(file, newPage, k - (numCellA + 1), content);
			}
			
			int tmp = getRightMost(file, curPage);
			setRightMost(file, newPage, tmp);
			
			long midLoc = getCellLoc(file, curPage, mid - 1);
			file.seek(midLoc);
			tmp = file.readInt();
			setRightMost(file, curPage, tmp);
			
			file.seek((newPage-1)*pageSize+2);
			file.writeShort(content);
			
			short enumOfSet = getCellOffset(file, curPage, numCellA-1);
			file.seek((curPage-1)*pageSize+2);
			file.writeShort(enumOfSet);

			
			int parent = getParent(file, curPage);
			setParent(file, newPage, parent);
			
			byte varNum = (byte) numCellA;
			setCellNumber(file, curPage, varNum);
			varNum = (byte) numCellB;
			setCellNumber(file, newPage, varNum);
			
		}catch(Exception ex){
			System.out.println(ex);
		}
	}

	
	public static void splitLeaf(RandomAccessFile file, int datPge){
		int newPage = makeLeafPage(file);
		int midKey = findMidKey(file, datPge);
		splitLeafPage(file, datPge, newPage);
		int parent = getParent(file, datPge);
		if(parent == 0){
			int rootPage = makeInteriorPage(file);
			setParent(file, datPge, rootPage);
			setParent(file, newPage, rootPage);
			setRightMost(file, rootPage, newPage);
			insertInteriorCell(file, rootPage, datPge, midKey);
		}else{
			long ploc = getPointerLoc(file, datPge, parent);
			setPointerLoc(file, ploc, parent, newPage);
			insertInteriorCell(file, parent, datPge, midKey);
			sortCellArray(file, parent);
			while(checkInteriorSpace(file, parent)){
				parent = splitInterior(file, parent);
			}
		}
	}

	public static int splitInterior(RandomAccessFile file, int datPge){
		int newPage = makeInteriorPage(file);
		int midKey = findMidKey(file, datPge);
		splitInteriorPage(file, datPge, newPage);
		int parent = getParent(file, datPge);
		if(parent == 0){
			int rootPage = makeInteriorPage(file);
			setParent(file, datPge, rootPage);
			setParent(file, newPage, rootPage);
			setRightMost(file, rootPage, newPage);
			insertInteriorCell(file, rootPage, datPge, midKey);
			return rootPage;
		}else{
			long ploc = getPointerLoc(file, datPge, parent);
			setPointerLoc(file, ploc, parent, newPage);
			insertInteriorCell(file, parent, datPge, midKey);
			sortCellArray(file, parent);
			return parent;
		}
	}

	
	public static void sortCellArray(RandomAccessFile file, int datPge){
		 byte varNum = getCellNumber(file, datPge);
		 int[] arrOfKey = getKeyArray(file, datPge);
		 short[] arrayOfCell = getCellArray(file, datPge);
		 int tmpLef;
		 short rmpRt;

		 for (int k = 1; k < varNum; k++) {
            for(int l = k ; l > 0 ; l--){
                if(arrOfKey[l] < arrOfKey[l-1]){

                    tmpLef = arrOfKey[l];
                    arrOfKey[l] = arrOfKey[l-1];
                    arrOfKey[l-1] = tmpLef;

                    rmpRt = arrayOfCell[l];
                    arrayOfCell[l] = arrayOfCell[l-1];
                    arrayOfCell[l-1] = rmpRt;
                }
            }
         }

         try{
         	file.seek((datPge-1)*pageSize+12);
         	for(int k = 0; k < varNum; k++){
				file.writeShort(arrayOfCell[k]);
			}
         }catch(Exception ex){
         	System.out.println("Error at sortCellArray");
         }
	}

	public static int[] getKeyArray(RandomAccessFile file, int datPge){
		int varNum = new Integer(getCellNumber(file, datPge));
		int[] lstArr = new int[varNum];

		try{
			file.seek((datPge-1)*pageSize);
			byte pageType = file.readByte();
			byte enumOfSet = 0;
			switch(pageType){
			    case 0x0d:
				    enumOfSet = 2;
				    break;
				case 0x05:
					enumOfSet = 4;
					break;
				default:
					enumOfSet = 2;
					break;
			}

			for(int k = 0; k < varNum; k++){
				long loc = getCellLoc(file, datPge, k);
				file.seek(loc+enumOfSet);
				lstArr[k] = file.readInt();
			}

		}catch(Exception ex){
			System.out.println(ex);
		}

		return lstArr;
	}
	
	public static short[] getCellArray(RandomAccessFile file, int datPge){
		int varNum = new Integer(getCellNumber(file, datPge));
		short[] lstArr = new short[varNum];

		try{
			file.seek((datPge-1)*pageSize+12);
			for(int k = 0; k < varNum; k++){
				lstArr[k] = file.readShort();
			}
		}catch(Exception ex){
			System.out.println(ex);
		}

		return lstArr;
	}

	
	public static long getPointerLoc(RandomAccessFile file, int datPge, int parent){
		long vlu = 0;
		try{
			int numCells = new Integer(getCellNumber(file, parent));
			for(int k=0; k < numCells; k++){
				long loc = getCellLoc(file, parent, k);
				file.seek(loc);
				int childPage = file.readInt();
				if(childPage == datPge){
					vlu = loc;
				}
			}
		}catch(Exception ex){
			System.out.println(ex);
		}

		return vlu;
	}

	public static void setPointerLoc(RandomAccessFile file, long loc, int parent, int datPge){
		try{
			if(loc == 0){
				file.seek((parent-1)*pageSize+4);
			}else{
				file.seek(loc);
			}
			file.writeInt(datPge);
		}catch(Exception ex){
			System.out.println(ex);
		}
	} 

	
	public static void insertInteriorCell(RandomAccessFile file, int datPge, int child, int key){
		try{
			
			file.seek((datPge-1)*pageSize+2);
			short content = file.readShort();
			
			if(content == 0)
				content = 512;
			
			content = (short)(content - 8);
			
			file.seek((datPge-1)*pageSize+content);
			file.writeInt(child);
			file.writeInt(key);
			
			file.seek((datPge-1)*pageSize+2);
			file.writeShort(content);
			
			byte varNum = getCellNumber(file, datPge);
			setCellOffset(file, datPge ,varNum, content);
			
			varNum = (byte) (varNum + 1);
			setCellNumber(file, datPge, varNum);

		}catch(Exception ex){
			System.out.println(ex);
		}
	}

	public static void insertLeafCell(RandomAccessFile dataFl, int datPge, int enumOfSet, short plsize, int key, byte[] stc, String[] vals){
		try{
			String d_sub;
			dataFl.seek((datPge-1)*pageSize+enumOfSet);
			dataFl.writeShort(plsize);
			dataFl.writeInt(key);
			int col = vals.length - 1;
			dataFl.writeByte(col);
			dataFl.write(stc);
			for(int k = 1; k < vals.length; k++){
				switch(stc[k-1]){
					case 0x00:
						dataFl.writeByte(0);
						break;
					case 0x01:
						dataFl.writeShort(0);
						break;
					case 0x02:
						dataFl.writeInt(0);
						break;
					case 0x03:
						dataFl.writeLong(0);
						break;
					case 0x04:
						dataFl.writeByte(new Byte(vals[k]));
						break;
					case 0x05:
						dataFl.writeShort(new Short(vals[k]));
						break;
					case 0x06:
						dataFl.writeInt(new Integer(vals[k]));
						break;
					case 0x07:
						dataFl.writeLong(new Long(vals[k]));
						break;
					case 0x08:
						dataFl.writeFloat(new Float(vals[k]));
						break;
					case 0x09:
						dataFl.writeDouble(new Double(vals[k]));
						break;
					case 0x0A:
						d_sub = vals[k];
						Date temp = new SimpleDateFormat(datePattern).parse(d_sub.substring(1, d_sub.length()-1));
						long time = temp.getTime();
						dataFl.writeLong(time);
						break;
					case 0x0B:
						d_sub = vals[k];
						d_sub = d_sub.substring(1, d_sub.length()-1);
						d_sub = d_sub+"_00:00:00";
						Date temp2 = new SimpleDateFormat(datePattern).parse(d_sub);
						long time2 = temp2.getTime();
						dataFl.writeLong(time2);
						break;
					default:
						dataFl.writeBytes(vals[k]);
						break;
				}
			}
			int n = getCellNumber(dataFl, datPge);
			byte tmp = (byte) (n+1);
			setCellNumber(dataFl, datPge, tmp);
			dataFl.seek((datPge-1)*pageSize+12+n*2);
			dataFl.writeShort(enumOfSet);
			dataFl.seek((datPge-1)*pageSize+2);
			int content = dataFl.readShort();
			if(content >= enumOfSet || content == 0){
				dataFl.seek((datPge-1)*pageSize+2);
				dataFl.writeShort(enumOfSet);
			}
		}catch(Exception ex){
			System.out.println(ex);
		}
	}

	public static void updateLeafCell(RandomAccessFile dataFl, int datPge, int enumOfSet, int plsize, int key, byte[] stc, String[] vals){
		try{
			String d_sub;
			dataFl.seek((datPge-1)*pageSize+enumOfSet);
			dataFl.writeShort(plsize);
			dataFl.writeInt(key);
			int col = vals.length - 1;
			dataFl.writeByte(col);
			dataFl.write(stc);
			for(int k = 1; k < vals.length; k++){
				switch(stc[k-1]){
					case 0x00:
						dataFl.writeByte(0);
						break;
					case 0x01:
						dataFl.writeShort(0);
						break;
					case 0x02:
						dataFl.writeInt(0);
						break;
					case 0x03:
						dataFl.writeLong(0);
						break;
					case 0x04:
						dataFl.writeByte(new Byte(vals[k]));
						break;
					case 0x05:
						dataFl.writeShort(new Short(vals[k]));
						break;
					case 0x06:
						dataFl.writeInt(new Integer(vals[k]));
						break;
					case 0x07:
						dataFl.writeLong(new Long(vals[k]));
						break;
					case 0x08:
						dataFl.writeFloat(new Float(vals[k]));
						break;
					case 0x09:
						dataFl.writeDouble(new Double(vals[k]));
						break;
					case 0x0A:
						d_sub = vals[k];
						Date temp = new SimpleDateFormat(datePattern).parse(d_sub.substring(1, d_sub.length()-1));
						long time = temp.getTime();
						dataFl.writeLong(time);
						break;
					case 0x0B:
						d_sub = vals[k];
						d_sub = d_sub.substring(1, d_sub.length()-1);
						d_sub = d_sub+"_00:00:00";
						Date temp2 = new SimpleDateFormat(datePattern).parse(d_sub);
						long time2 = temp2.getTime();
						dataFl.writeLong(time2);
						break;
					default:
						dataFl.writeBytes(vals[k]);
						break;
				}
			}
		}catch(Exception ex){
			System.out.println(ex);
		}
	}

	
	public static boolean checkInteriorSpace(RandomAccessFile dataFl, int datPge){
		byte numCells = getCellNumber(dataFl, datPge);
		if(numCells > 30)
			return true;
		else
			return false;
	}

	public static int checkLeafSpace(RandomAccessFile dataFl, int datPge, int size){
		int vlu = -1;

		try{
			dataFl.seek((datPge-1)*pageSize+2);
			int content = dataFl.readShort();
			if(content == 0)
				return pageSize - size;
			int numCells = getCellNumber(dataFl, datPge);
			int space = content - 20 - 2*numCells;
			if(size < space)
				return content - size;
			
		}catch(Exception ex){
			System.out.println(ex);
		}

		return vlu;
	}

	
	public static int getParent(RandomAccessFile dataFl, int datPge){
		int vlu = 0;

		try{
			dataFl.seek((datPge-1)*pageSize+8);
			vlu = dataFl.readInt();
		}catch(Exception ex){
			System.out.println(ex);
		}

		return vlu;
	}

	public static void setParent(RandomAccessFile dataFl, int datPge, int parent){
		try{
			dataFl.seek((datPge-1)*pageSize+8);
			dataFl.writeInt(parent);
		}catch(Exception ex){
			System.out.println(ex);
		}
	}
	
	public static int getRightMost(RandomAccessFile dataFl, int datPge){
		int rl = 0;

		try{
			dataFl.seek((datPge-1)*pageSize+4);
			rl = dataFl.readInt();
		}catch(Exception ex){
			System.out.println("Error at getRightMost");
		}

		return rl;
	}

	public static void setRightMost(RandomAccessFile dataFl, int datPge, int rightLeaf){

		try{
			dataFl.seek((datPge-1)*pageSize+4);
			dataFl.writeInt(rightLeaf);
		}catch(Exception ex){
			System.out.println("Error at setRightMost");
		}

	}

	public static boolean hasKey(RandomAccessFile dataFl, int datPge, int key){
		int[] lstOfKys = getKeyArray(dataFl, datPge);
		for(int k : lstOfKys)
			if(key == k)
				return true;
		return false;
	}
	
	public static long getCellLoc(RandomAccessFile dataFl, int datPge, int id){
		long loc = 0;
		try{
			dataFl.seek((datPge-1)*pageSize+12+id*2);
			short enumOfSet = dataFl.readShort();
			long d_orig = (datPge-1)*pageSize;
			loc = d_orig + enumOfSet;
		}catch(Exception ex){
			System.out.println(ex);
		}
		return loc;
	}

	public static byte getCellNumber(RandomAccessFile dataFl, int datPge){
		byte vlu = 0;

		try{
			dataFl.seek((datPge-1)*pageSize+1);
			vlu = dataFl.readByte();
		}catch(Exception ex){
			System.out.println(ex);
		}

		return vlu;
	}

	public static void setCellNumber(RandomAccessFile dataFl, int datPge, byte varNum){
		try{
			dataFl.seek((datPge-1)*pageSize+1);
			dataFl.writeByte(varNum);
		}catch(Exception ex){
			System.out.println(ex);
		}
	}
	
	public static short getCellOffset(RandomAccessFile dataFl, int datPge, int id){
		short enumOfSet = 0;
		try{
			dataFl.seek((datPge-1)*pageSize+12+id*2);
			enumOfSet = dataFl.readShort();
		}catch(Exception ex){
			System.out.println(ex);
		}
		return enumOfSet;
	}

	public static void setCellOffset(RandomAccessFile dataFl, int datPge, int id, int enumOfSet){
		try{
			dataFl.seek((datPge-1)*pageSize+12+id*2);
			dataFl.writeShort(enumOfSet);
		}catch(Exception ex){
			System.out.println(ex);
		}
	}
    
	public static byte getPageType(RandomAccessFile dataFl, int datPge){
		byte dTyp=0x05;
		try {
			dataFl.seek((datPge-1)*pageSize);
			dTyp = dataFl.readByte();
		} catch (Exception ex) {
			System.out.println(ex);
		}
		return dTyp;
	}
	//public static void main(String[] args){}
}















