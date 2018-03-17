package org.angularbaby.test;

import org.json.JSONArray;
import org.json.JSONObject;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

public class JsonUtil {
    /**
     * 把ResultSet集合转换成JsonArray数组
     * @param rs
     * @return
     * @throws Exception
     */
    public static JSONArray formatRsToJsonArray(ResultSet rs) throws Exception{
        // 获取表结构
        ResultSetMetaData md = rs.getMetaData();
        // 得到行的总数
        int num = md.getColumnCount();
        // json数组，根据下标找值；[{name1:wp},{name2:{name3:'ww'}}] name为key值，wp为value值
        JSONArray jsonArray = new JSONArray();
        // JSONArray array=JSONArray.fromObject(String);将String转换为JSONArray格式
        while (rs.next()) {
          // 创建json对象 保存字段名及对应的值
          JSONObject mapOfColValues = new JSONObject();
          for (int i = 1; i <= num; i++) {
            // 添加键值对
            mapOfColValues.put(md.getColumnName(i), rs.getObject(i));
          }
          // System.out.println(mapOfColValues.toString());
          jsonArray.put(mapOfColValues);
        }
        return jsonArray;
     }
}