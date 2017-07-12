package DBPJ;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
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
		Create("SJTU");
		long endTime=System.currentTimeMillis();
		System.out.println("建表时间： "+(endTime - startTime)+"ms");
		
		startTime=System.currentTimeMillis();
		Write("SJTU");
		endTime=System.currentTimeMillis();
		System.out.println("写入时间： "+(endTime - startTime)+"ms");

		startTime=System.currentTimeMillis();
		Select("SJTU");
		endTime=System.currentTimeMillis();
		System.out.println("大量单条查询时间： "+(endTime - startTime)+"ms");
		
		startTime=System.currentTimeMillis();
		scaner("SJTU");
		endTime=System.currentTimeMillis();
		System.out.println("scan全表时间： "+(endTime - startTime)+"ms");
	}
	
	private static void Write(String tablename) throws IOException{
		for(int id = 1000000000; id < 1000000000 + 100000; id++){
			writeRow(tablename, String.valueOf(id));
		}
		System.out.println("写入成功");
	}
	
	private static void Select(String tablename) throws IOException{
		selectRow(tablename, String.valueOf(1000000000));
		for(int id = 1000000000; id < 1000000000 + 100000; id++){
			selectRow(tablename, String.valueOf(id));
		}
		System.out.println("查询成功");
	}
	
	private static Configuration conf =null;
    static {
        conf = HBaseConfiguration.create();
    }
    
    private static void Create(String tablename) throws IOException{
		HBaseAdmin admin =new HBaseAdmin(conf);
		if (admin.tableExists(tablename)) {
			return;
		}
		
		HTableDescriptor tableDesc =new HTableDescriptor(tablename);
		tableDesc.addFamily(new HColumnDescriptor("name"));
		tableDesc.addFamily(new HColumnDescriptor("age"));
		tableDesc.addFamily(new HColumnDescriptor("sex"));
		tableDesc.addFamily(new HColumnDescriptor("math"));
		tableDesc.addFamily(new HColumnDescriptor("english"));
		tableDesc.addFamily(new HColumnDescriptor("OOT"));
		admin.createTable(tableDesc);
		System.out.println("表创建成功！");
	}
    
    private static void writeRow(String tablename, String rowkey) throws IOException{
    	HTable table =new HTable(conf, tablename);
    	Put put =new Put(Bytes.toBytes(rowkey));
    	put.add(Bytes.toBytes ("name"),Bytes.toBytes("Quali"), Bytes.toBytes("chiya"));
    	put.add(Bytes.toBytes ("age"),Bytes.toBytes("Quali"), Bytes.toBytes("17"));
    	put.add(Bytes.toBytes ("sex"),Bytes.toBytes("Quali"), Bytes.toBytes("M"));
    	put.add(Bytes.toBytes ("math"),Bytes.toBytes("Quali"), Bytes.toBytes("100"));
    	put.add(Bytes.toBytes ("english"),Bytes.toBytes("Quali"), Bytes.toBytes("100"));
    	put.add(Bytes.toBytes ("OOT"),Bytes.toBytes("Quali"), Bytes.toBytes("100"));
    	table.put(put);
    	table.close();
    }
    
    private static void selectRow(String tablename, String rowkey) throws IOException{
    	HTable table = new HTable(conf, tablename);
    	Get get = new Get(rowkey.getBytes());
    	Result rs = table.get(get);
//    	for (KeyValue kv : rs.raw()) {
//            System.out.print(new String(kv.getRow()) +" ");
//            System.out.print(new String(kv.getFamily()) +":");
//            System.out.print(new String(kv.getQualifier()) +" ");
//            System.out.print(kv.getTimestamp() +" ");
//            System.out.println(new String(kv.getValue()));
//        }
    	table.close();
    }
    
    private static void scaner(String tablename) throws IOException{
    	HTable table =new HTable(conf, tablename);
    	Scan s =new Scan();
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
