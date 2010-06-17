package rpm;

import java.io.IOException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class client {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		HBaseConfiguration config = new HBaseConfiguration();
		HBaseAdmin admin = new HBaseAdmin(config);
		if  (!admin.tableExists("myLittleHBaseTable")) 
		{
			HTableDescriptor tdesc = new HTableDescriptor("myLittleHBaseTable");
			HColumnDescriptor cdesc = new HColumnDescriptor("myLittleFamily"); 
			tdesc.addFamily(cdesc);
			admin.createTable(tdesc);
		}
		else
		{
			System.out.println("Table exists, you are lucky ;)");			
			
		}
		
		HTable table = new HTable(config, "myLittleHBaseTable");
		Put p = new Put(Bytes.toBytes("myLittleRow"));
		p.add(Bytes.toBytes("myLittleFamily"), Bytes.toBytes("someQualifier"), Bytes.toBytes("Some Value"));
		table.put(p);
		
		Get g = new Get(Bytes.toBytes("myLittleRow"));
		Result r = table.get(g);
	    byte [] value = r.getValue(Bytes.toBytes("myLittleFamily"), Bytes.toBytes("someQualifier"));
	    // If we convert the value bytes, we should get back 'Some Value', the
	    // value we inserted at this location.
	    String valueStr = Bytes.toString(value);
	    System.out.println("GET: " + valueStr);

	    Scan s = new Scan();
	    s.addColumn(Bytes.toBytes("myLittleFamily"), Bytes.toBytes("someQualifier"));
	    ResultScanner scanner = table.getScanner(s);
	    try {
	        // Scanners return Result instances.
	        // Now, for the actual iteration. One way is to use a while loop like so:
	        for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
	          // print out the row we found and the columns we were looking for
	          System.out.println("Found row: " + rr);
	        }

	        // The other approach is to use a foreach loop. Scanners are iterable!
	        // for (Result rr : scanner) {
	        //   System.out.println("Found row: " + rr);
	        // }
	      } finally {
	        // Make sure you close your scanners when you are done!
	        // Thats why we have it inside a try/finally clause
	        scanner.close();
	      }
	    
	}

}
