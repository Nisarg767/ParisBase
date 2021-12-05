import java.io.RandomAccessFile;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Scanner;
import java.util.SortedMap;
public class Index {
    public static void createIndex(String tblName, String columnName, String dtaType) {
        int serlCode = 0;
        int recrdSize = 0;
        if (dtaType.equalsIgnoreCase("int")) {
            recrdSize = recrdSize + 4;
            serlCode = 0x06;
        } else if (dtaType.equalsIgnoreCase("tinyint")) {
            recrdSize = recrdSize + 1;
            serlCode = 0x04;
        } else if (dtaType.equalsIgnoreCase("smallint")) {
            recrdSize = recrdSize + 2;
            serlCode = 0x05;
        } else if (dtaType.equalsIgnoreCase("bigint")) {
            recrdSize = recrdSize + 8;
            serlCode = 0x07;
        } else if (dtaType.equalsIgnoreCase("real")) {
            recrdSize = recrdSize + 4;
            serlCode = 0x08;
        } else if (dtaType.equalsIgnoreCase("double")) {
            recrdSize = recrdSize + 8;
            serlCode = 0x09;
        } else if (dtaType.equalsIgnoreCase("datetime")) {
            recrdSize = recrdSize + 8;
            serlCode = 0x0A;
        } else if (dtaType.equalsIgnoreCase("date")) {
            recrdSize = recrdSize + 8;
            serlCode = 0x0B;
        } else if (dtaType.equalsIgnoreCase("text")) {
            //recrdSize=recrdSize+8;
            serlCode = 0x0C;
        }
        int ordinalPosition1 = 0;
        ArrayList < String > ordPos = new ArrayList < > ();

        for (int oP = 0; oP < 16; oP++) {
            ordPos.add(".");
        }

        try {
            RandomAccessFile columnFile1 = new RandomAccessFile("data/" + columnName + ".ndx", "rw");
            columnFile1.setLength(512);

            columnFile1.seek(0);
            columnFile1.writeByte(serlCode);
            int null_val = 1;

            columnFile1.writeByte(null_val);

            columnFile1.write(ordinalPosition1);
            ordinalPosition1++;
            columnFile1.writeByte(0x00);
            columnFile1.writeByte(0x10);
        } catch (Exception ex) {
            System.out.println(ex);
        }
    }
}