package FDU;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class hbase {

	
	
	public static void main(String[] args) throws IOException{
		long startTime=System.currentTimeMillis();
		Create("FDU");
		long endTime=System.currentTimeMillis();
		System.out.println("建表时间： "+(endTime - startTime)+"ms");
		
		startTime=System.currentTimeMillis();
		Write("FDU");
		endTime=System.currentTimeMillis();
		System.out.println("写入时间： "+(endTime - startTime)+"ms");

		startTime=System.currentTimeMillis();
		Select("FDU");
		endTime=System.currentTimeMillis();
		System.out.println("大量单条查询时间： "+(endTime - startTime)+"ms");
		
		startTime=System.currentTimeMillis();
		scaner("FDU");
		endTime=System.currentTimeMillis();
		System.out.println("scan全表时间： "+(endTime - startTime)+"ms");
	}
	
	private static void Write(String tablename) throws IOException{
//		final HTable[] wTableLog = new HTable[10];
//		for(int i = 0; i < 10; i++){
//			wTableLog[i] = new HTable(conf, tablename);
//			wTableLog[i].setWriteBufferSize(5 * 1024 * 1024);
//			wTableLog[i].setAutoFlush(false);
//		}
		HTable table =new HTable(conf, tablename);
		table.setWriteBufferSize(6 * 1024 * 1024);
		table.setAutoFlush(false);
		for(int lowid = 1000000000; lowid < 1000000000 + 1000000; lowid += 1000){
			int highid = lowid + 1000;
			List<Put> puts = new ArrayList<Put>();
			for (int id = lowid; id < highid; id++){
		    	Put put =new Put(Bytes.toBytes(String.valueOf(id)));
		    	put.setWriteToWAL(false);
		    	put.add(Bytes.toBytes ("info"),Bytes.toBytes("name"), Bytes.toBytes("chiya"));
		    	put.add(Bytes.toBytes ("info"),Bytes.toBytes("age"), Bytes.toBytes("17"));
		    	put.add(Bytes.toBytes ("info"),Bytes.toBytes("sex"), Bytes.toBytes("M"));
		    	put.add(Bytes.toBytes ("score"),Bytes.toBytes("math"), Bytes.toBytes("100"));
		    	put.add(Bytes.toBytes ("score"),Bytes.toBytes("english"), Bytes.toBytes("100"));
		    	put.add(Bytes.toBytes ("score"),Bytes.toBytes("OOT"), Bytes.toBytes("100"));
		    	puts.add(put);
			}
			table.put(puts);
		}
    	table.close();
		System.out.println("写入成功");
	}
	
	private static void Select(String tablename) throws IOException{
		HTable table = new HTable(conf, tablename);
		for(int lowid = 1000000000; lowid < 1000000000 + 1000000; lowid += 1000){
			int highid = lowid + 1000;
			List<Get> gets = new ArrayList<Get>();
			for (int id = lowid; id < highid; id++){
				Get get = new Get(Bytes.toBytes(String.valueOf(id)));
				gets.add(get);
			}
	    	Result[] rs = table.get(gets);
		}
		System.out.println("查询成功");
	}
	
	private static Configuration conf =null;
    static {
        conf = HBaseConfiguration.create();
    }
    
//    private static void Create(String tablename) throws IOException{
//		HBaseAdmin admin =new HBaseAdmin(conf);
//		if (admin.tableExists(tablename)) {
//			return;
//		}
//		
//		HTableDescriptor tableDesc =new HTableDescriptor(tablename);
//		tableDesc.addFamily(new HColumnDescriptor("info"));
//		tableDesc.addFamily(new HColumnDescriptor("score"));
//		admin.createTable(tableDesc);
//		System.out.println("表创建成功！");
//	}
	public static boolean createTable(HBaseAdmin admin, HTableDescriptor table, byte[][] splits)
	throws IOException {
	  try {
	    admin.createTable(table, splits);
	    return true;
	  } catch (TableExistsException e) {
	    System.out.println("table " + table.getNameAsString() + " already exists");
	    // the table already exists...
	    return false;  
	  }
	}

	public static byte[][] getHexSplits(String startKey, String endKey, int numRegions) {
	  byte[][] splits = new byte[numRegions-1][];
	  BigInteger lowestKey = new BigInteger(startKey, 16);
	  BigInteger highestKey = new BigInteger(endKey, 16);
	  BigInteger range = highestKey.subtract(lowestKey);
	  BigInteger regionIncrement = range.divide(BigInteger.valueOf(numRegions));
	  lowestKey = lowestKey.add(regionIncrement);
	  for(int i=0; i < numRegions-1;i++) {
	    BigInteger key = lowestKey.add(regionIncrement.multiply(BigInteger.valueOf(i)));
	    byte[] b = String.format("%016x", key).getBytes();
	    splits[i] = b;
	  }
	  return splits;
	}
    		
    private static void Create(String tablename) throws IOException{
		HBaseAdmin admin =new HBaseAdmin(conf);
		if (admin.tableExists(tablename)) {
			return;
		}
		
		HTableDescriptor tableDesc =new HTableDescriptor(tablename);
		tableDesc.addFamily(new HColumnDescriptor("info"));
		tableDesc.addFamily(new HColumnDescriptor("score"));
		byte[][] splits = getHexSplits("1000000000","2000000000",300);
		createTable(admin, tableDesc,splits);
		System.out.println("表创建成功！");
	}
    
    private static void writeRow(String tablename, String rowkey) throws IOException{
    	HTable table =new HTable(conf, tablename);
    	Put put =new Put(Bytes.toBytes(rowkey));
    	put.add(Bytes.toBytes ("info"),Bytes.toBytes("name"), Bytes.toBytes("chiya"));
    	put.add(Bytes.toBytes ("info"),Bytes.toBytes("age"), Bytes.toBytes("17"));
    	put.add(Bytes.toBytes ("info"),Bytes.toBytes("sex"), Bytes.toBytes("M"));
    	put.add(Bytes.toBytes ("score"),Bytes.toBytes("math"), Bytes.toBytes("100"));
    	put.add(Bytes.toBytes ("score"),Bytes.toBytes("english"), Bytes.toBytes("100"));
    	put.add(Bytes.toBytes ("score"),Bytes.toBytes("OOT"), Bytes.toBytes("100"));
    	table.put(put);
    	table.close();
    }
    
    private static void selectRow(String tablename, String rowkey) throws IOException{
    	HTable table = new HTable(conf, tablename);
    	Get get = new Get(rowkey.getBytes());
    	Result rs = table.get(get);
    	for (KeyValue kv : rs.raw()) {
            System.out.print(new String(kv.getRow()) +" ");
            System.out.print(new String(kv.getFamily()) +":");
            System.out.print(new String(kv.getQualifier()) +" ");
            System.out.print(kv.getTimestamp() +" ");
            System.out.println(new String(kv.getValue()));
        }
    	table.close();
    }
    
    private static void scaner(String tablename) throws IOException{
    	HTable table =new HTable(conf, tablename);
    	Scan s =new Scan();
    	s.setCaching(32768);
    	ResultScanner rs = table.getScanner(s);
    	for(Result r: rs){
    		for(KeyValue kv : r.raw()){
//    			System.out.print(new String(kv.getRow()) +" ");
//                System.out.print(new String(kv.getFamily()) +":");
//                System.out.print(new String(kv.getQualifier()) +" ");
//                System.out.print(kv.getTimestamp() +" ");
//                System.out.println(new String(kv.getValue()));
    		}
    	}
    	rs.close();
    	table.close();
    }
}
