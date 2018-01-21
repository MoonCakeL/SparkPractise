package sparkHbase.example.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HBaseUtils {
    static Configuration cfg;
    static Connection connection;
    static TableName htableName;
    static {
        
        try {
        	cfg = HBaseConfiguration.create();
            cfg.set("hbase.zookeeper.quorum", "192.168.42.24,192.168.42.25,192.168.42.26");
            connection = ConnectionFactory.createConnection(cfg);
        } catch (IOException e) {
            e.printStackTrace();
        }
        //System.out.println(cfg.get("hbase.master"));
    }

    //新建表
    public static boolean create(String tableName, String columnFamily)
            throws Exception {
        Admin admin = connection.getAdmin();

        if (admin.tableExists(TableName.valueOf(tableName))) {
            System.out.println(tableName + " exists!");
            return false;
        } else {
			TableDescriptorBuilder tableDesc =
					TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));

            String[] columnFamilyArray = columnFamily.split(",");
			for (int i=0;i<columnFamilyArray.length;i++){

				ColumnFamilyDescriptorBuilder columnFamilyDesc =
						ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(columnFamilyArray[i]));
				tableDesc.addColumnFamily(columnFamilyDesc.build());
			}

            admin.createTable(tableDesc.build());
            System.out.println(tableName + " create successfully!");
            return true;
        }
    }

    //插入数据
    public static int put(String tablename, String row, String columnFamily,
                              String qualifier, String data) throws Exception {
        Table table = connection.getTable(TableName.valueOf(tablename));
        Put put = new Put(Bytes.toBytes(row));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier),
                Bytes.toBytes(data));
        table.put(put);
        System.out.println("put '" + row + "', '" + columnFamily + ":" + qualifier
                + "', '" + data + "'");
        return put.size();
    }

    //把result转换成map，方便返回json数据
    public static Map<String, Object> resultToMap(Result result) {
        Map<String, Object> resMap = new HashMap<String, Object>();
        List<Cell> listCell = result.listCells();
        Map<String, Object> tempMap = new HashMap<String, Object>();
        String rowname = "";
        List<String> familynamelist = new ArrayList<String>();
        for (Cell cell : listCell) {
            byte[] rowArray = cell.getRowArray();
            byte[] familyArray = cell.getFamilyArray();
            byte[] qualifierArray = cell.getQualifierArray();
            byte[] valueArray = cell.getValueArray();
            int rowoffset = cell.getRowOffset();
            int familyoffset = cell.getFamilyOffset();
            int qualifieroffset = cell.getQualifierOffset();
            int valueoffset = cell.getValueOffset();
            int rowlength = cell.getRowLength();
            int familylength = cell.getFamilyLength();
            int qualifierlength = cell.getQualifierLength();
            int valuelength = cell.getValueLength();

            byte[] temprowarray = new byte[rowlength];
            System.arraycopy(rowArray, rowoffset, temprowarray, 0, rowlength);
            String temprow = Bytes.toString(temprowarray);
//            System.out.println(Bytes.toString(temprowarray));

            byte[] tempqulifierarray = new byte[qualifierlength];
            System.arraycopy(qualifierArray, qualifieroffset, tempqulifierarray, 0, qualifierlength);
            String tempqulifier = Bytes.toString(tempqulifierarray);
//            System.out.println(Bytes.toString(tempqulifierarray));

            byte[] tempfamilyarray = new byte[familylength];
            System.arraycopy(familyArray, familyoffset, tempfamilyarray, 0, familylength);
            String tempfamily = Bytes.toString(tempfamilyarray);
//            System.out.println(Bytes.toString(tempfamilyarray));

            byte[] tempvaluearray = new byte[valuelength];
            System.arraycopy(valueArray, valueoffset, tempvaluearray, 0, valuelength);
            String tempvalue = Bytes.toString(tempvaluearray);
//            System.out.println(Bytes.toString(tempvaluearray));


            tempMap.put(tempfamily + ":" + tempqulifier, tempvalue);
//            long t= cell.getTimestamp();
//            tempMap.put("timestamp",t);
            rowname = temprow;
            String familyname = tempfamily;
            if (familynamelist.indexOf(familyname) < 0) {
                familynamelist.add(familyname);
            }
        }
        resMap.put("rowname", rowname);
        for (String familyname : familynamelist) {
            HashMap<String, Object> tempFilterMap = new HashMap<String, Object>();
            for (String key : tempMap.keySet()) {
                String[] keyArray = key.split(":");
                if (keyArray[0].equals(familyname)) {
                    tempFilterMap.put(keyArray[1], tempMap.get(key));
                }
            }
            resMap.put(familyname, tempFilterMap);
        }

        return resMap;
    }

    //查看某行
    public static String get(String tablename, String row) throws Exception {
        Table table = connection.getTable(TableName.valueOf(tablename));
        Get get = new Get(Bytes.toBytes(row));
        Result result = table.get(get);
        System.out.println("Get: " + result);
        return resultToMap(result).toString();
    }

    //查看全表
    public static String scan(String tablename) throws Exception {
        Table table = connection.getTable(TableName.valueOf(tablename));
        Scan s = new Scan();
        ResultScanner rs = table.getScanner(s);

        List<Map<String, Object>> resList = new ArrayList<Map<String, Object>>();
        for (Result r : rs) {
            Map<String, Object> tempmap = resultToMap(r);
            resList.add(tempmap);
        }
        return resList.toString();
    }

    //删除表
    public static boolean delete(String tableName) throws IOException {
        HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
        String[] arraysTable = tableName.split(",");
        for (int i=0; i<arraysTable.length;i++){
        if (admin.tableExists(TableName.valueOf(arraysTable[i]))) {
				try {
					admin.disableTable(TableName.valueOf(arraysTable[i]));
					admin.deleteTable(TableName.valueOf(arraysTable[i]));
				} catch (Exception e) {
					e.printStackTrace();
					return false;
				}
			}
		}
        return true;

    }

    //删除ColumnFamily
    public static boolean deleteColumnFamily(String tableName, String columnFamilyName) throws IOException {
        HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
        if (admin.tableExists(TableName.valueOf(tableName))) {
            try {
                admin.deleteColumn(TableName.valueOf(tableName), Bytes.toBytes(columnFamilyName));
            } catch (Exception e) {
                e.printStackTrace();
                return false;
            }
        }
        return true;
    }

    //删除row
    public static boolean deleteRow(String tableName, String rowName) throws IOException {
        HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
        Table table = connection.getTable(TableName.valueOf(tableName));
        if (admin.tableExists(TableName.valueOf(tableName))) {
            try {
                Delete delete = new Delete(rowName.getBytes());
                table.delete(delete);
            } catch (Exception e) {
                e.printStackTrace();
                return false;
            }
        }
        return true;
    }

    //删除qualifier
    public static boolean deleteQualifier(String tableName, String rowName, String columnFamilyName, String qualifierName) throws IOException {
        HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
        Table table = connection.getTable(TableName.valueOf(tableName));
        if (admin.tableExists(TableName.valueOf(tableName))) {
            try {
                Delete delete = new Delete(rowName.getBytes());
                delete.addColumns(columnFamilyName.getBytes(), qualifierName.getBytes());
                table.delete(delete);
            } catch (Exception e) {
                e.printStackTrace();
                return false;
            }
        }
        return true;
    }

    //单个qualifier的值等于
    public static String queryequal(String tablename, String columnFamily, String qualifier, String data) throws Exception {
        //某列等于data的
        Filter filter = new SingleColumnValueFilter(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier),
                CompareFilter.CompareOp.EQUAL, Bytes.toBytes(data));
        FilterList filterList = new FilterList();
        filterList.addFilter(filter);

        return query(tablename, filterList);
    }

    //查询年龄在mindata和maxdata之间的数据
    public static String queryagebetween(String tablename, String columnFamily, String qualifier, String mindata, String maxdata) throws Exception {
        Filter filter = new SingleColumnValueFilter(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier),
                CompareFilter.CompareOp.LESS_OR_EQUAL, Bytes.toBytes(maxdata));
        Filter filter1 = new SingleColumnValueFilter(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier),
                CompareFilter.CompareOp.GREATER_OR_EQUAL, Bytes.toBytes(mindata));
        FilterList filterList = new FilterList();
        filterList.addFilter(filter);
        filterList.addFilter(filter1);
        return query(tablename, filterList);
    }

    //过滤列
    public static String querycolumn(String tablename, String prefix) throws Exception {
        Filter filter = new ColumnPrefixFilter(Bytes.toBytes(prefix));
        FilterList filterList = new FilterList();
        filterList.addFilter(filter);
        return query(tablename, filterList);
    }

    //得到分页
    public static String queryRowCount(String tablename, String startrowname, String count) throws Exception {
        Filter filter = new PageFilter(Integer.parseInt(count));
        Filter filter1 = new SingleColumnValueFilter(Bytes.toBytes("author"), Bytes.toBytes("age"),
                CompareFilter.CompareOp.EQUAL, Bytes.toBytes("30"));
        FilterList filterList = new FilterList();
//        filterList.addFilter(filter1);
        filterList.addFilter(filter);

        Table table = connection.getTable(TableName.valueOf(tablename));
        Scan s = new Scan(Bytes.toBytes(startrowname));
        s.setFilter(filterList);
        ResultScanner rs = table.getScanner(s);

        List<Map<String, Object>> resList = new ArrayList<Map<String, Object>>();
        for (Result r : rs) {
            Map<String, Object> tempmap = resultToMap(r);
            resList.add(tempmap);
        }

        return resList.toString();
    }

    //所有查询都需要的公共query方法
    public static String query(String tablename, FilterList filterList) throws Exception {
        Table table = connection.getTable(TableName.valueOf(tablename));
        Scan s = new Scan();
        s.setFilter(filterList);
        ResultScanner rs = table.getScanner(s);

        List<Map<String, Object>> resList = new ArrayList<Map<String, Object>>();
        for (Result r : rs) {
            Map<String, Object> tempmap = resultToMap(r);
            resList.add(tempmap);
        }
        return resList.toString();
    }

    //测试用，添加数据
    public static boolean testInsertdata() throws Exception {
        Table table = connection.getTable(TableName.valueOf("test"));
        for (int i = 20; i < 12000; i++) {
            Put put = new Put(Bytes.toBytes("r" + String.valueOf(i)));
            put.addColumn(Bytes.toBytes("author"), Bytes.toBytes("age"),
                    Bytes.toBytes(String.valueOf(i)));
            table.put(put);
            System.out.println("put '" + "r" + String.valueOf(i)+ "', '" + "author" + ":" + "age"
                    + "', '" + String.valueOf(i) + "'");
        }
        return true;
    }

    //测试用删除数据
    public static boolean deleteInsertdata() throws Exception{
        HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
        Table table = connection.getTable(TableName.valueOf("test"));
        if (admin.tableExists(TableName.valueOf("test"))) {
            try {
                for (int i = 20; i < 12000; i++) {
                    Delete delete = new Delete(("r" + String.valueOf(i)).getBytes());
                    table.delete(delete);
                }
            } catch (Exception e) {
                e.printStackTrace();
                return false;
            }
        }
        return true;
    }
}

