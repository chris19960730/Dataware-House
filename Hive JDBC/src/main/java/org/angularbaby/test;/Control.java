package org.angularbaby.test;

import org.json.JSONArray;
import org.springframework.web.bind.annotation.*;

import java.sql.*;

@RestController
public class Control {
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";//jdbc驱动路径
    private static String url = "jdbc:hive2://localhost:10000/hive";//hive库地址+库名
    private static String user = "hadoop";//用户名
    private static String password = "1111";//密码
    private static String sql = "";
    private static ResultSet res;


    @RequestMapping("/")
    public String index() {
        return "hello";
    }

    @RequestMapping("/reviews")
    public String reviews() {
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = getConn();
            System.out.println(conn);
            stmt = conn.createStatement();
            String tableName="review";//hive表名
            sql = "select * from " + tableName+" limit 0,100";
            System.out.println("Running:" + sql);
            res = stmt.executeQuery(sql);
            System.out.println("执行 select * query 运行结果:");
            JSONArray jsonArray = org.angularbaby.test.JsonUtil.formatRsToJsonArray(res);

            return jsonArray.toString();
            //while (res.next()) {
            //System.out.println(res.getString(1) + "\t" + res.getString(1));
            //}

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        } finally {
            try {
                if(stmt!=null){
                    stmt.close();
                    stmt=null;
                }
                if (conn != null) {
                    conn.close();
                    conn = null;
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return "helloworld";
    }

    @RequestMapping("/movies")
    public String movies() {
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = getConn();
            System.out.println(conn);
            stmt = conn.createStatement();
            String tableName="movies";//hive表名
            sql = "select * from " + tableName+" limit 0,100";
            System.out.println("Running:" + sql);
            res = stmt.executeQuery(sql);
            System.out.println("执行 select * query 运行结果:");
            JSONArray jsonArray = org.angularbaby.test.JsonUtil.formatRsToJsonArray(res);

            return jsonArray.toString();
            //while (res.next()) {
                //System.out.println(res.getString(1) + "\t" + res.getString(1));
            //}

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        } finally {
            try {
                if(stmt!=null){
                    stmt.close();
                    stmt=null;
                }
                if (conn != null) {
                    conn.close();
                    conn = null;
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return "helloworld";
    }

    @RequestMapping("/actors")
    public String actors() {
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = getConn();
            System.out.println(conn);
            stmt = conn.createStatement();
            String tableName="actors";//hive表名
            sql = "select * from " + tableName+" limit 0,100";
            System.out.println("Running:" + sql);
            res = stmt.executeQuery(sql);
            System.out.println("执行 select * query 运行结果:");
            JSONArray jsonArray = org.angularbaby.test.JsonUtil.formatRsToJsonArray(res);

            return jsonArray.toString();
            //while (res.next()) {
            //System.out.println(res.getString(1) + "\t" + res.getString(1));
            //}

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        } finally {
            try {
                if(stmt!=null){
                    stmt.close();
                    stmt=null;
                }
                if (conn != null) {
                    conn.close();
                    conn = null;
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return "helloworld";
    }

    @RequestMapping("/writers")
    public String writers() {
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = getConn();
            System.out.println(conn);
            stmt = conn.createStatement();
            String tableName="writers";//hive表名
            sql = "select * from " + tableName+" limit 0,100";
            System.out.println("Running:" + sql);
            res = stmt.executeQuery(sql);
            System.out.println("执行 select * query 运行结果:");
            JSONArray jsonArray = org.angularbaby.test.JsonUtil.formatRsToJsonArray(res);

            return jsonArray.toString();
            //while (res.next()) {
            //System.out.println(res.getString(1) + "\t" + res.getString(1));
            //}

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        } finally {
            try {
                if(stmt!=null){
                    stmt.close();
                    stmt=null;
                }
                if (conn != null) {
                    conn.close();
                    conn = null;
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return "helloworld";
    }

    @RequestMapping("/producers")
    public String producers() {
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = getConn();
            System.out.println(conn);
            stmt = conn.createStatement();
            String tableName="producers";//hive表名
            sql = "select * from " + tableName+" limit 0,100";
            System.out.println("Running:" + sql);
            res = stmt.executeQuery(sql);
            System.out.println("执行 select * query 运行结果:");
            JSONArray jsonArray = org.angularbaby.test.JsonUtil.formatRsToJsonArray(res);

            return jsonArray.toString();
            //while (res.next()) {
            //System.out.println(res.getString(1) + "\t" + res.getString(1));
            //}

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        } finally {
            try {
                if(stmt!=null){
                    stmt.close();
                    stmt=null;
                }
                if (conn != null) {
                    conn.close();
                    conn = null;
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return "helloworld";
    }

    @RequestMapping("/people")
    public String people() {
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = getConn();
            System.out.println(conn);
            stmt = conn.createStatement();
            String tableName="people";//hive表名
            sql = "select * from " + tableName+" limit 0,100";
            System.out.println("Running:" + sql);
            res = stmt.executeQuery(sql);
            System.out.println("执行 select * query 运行结果:");
            JSONArray jsonArray = org.angularbaby.test.JsonUtil.formatRsToJsonArray(res);

            return jsonArray.toString();
            //while (res.next()) {
            //System.out.println(res.getString(1) + "\t" + res.getString(1));
            //}

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        } finally {
            try {
                if(stmt!=null){
                    stmt.close();
                    stmt=null;
                }
                if (conn != null) {
                    conn.close();
                    conn = null;
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return "helloworld";
    }

    @RequestMapping("/creators")
    public String creators() {
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = getConn();
            System.out.println(conn);
            stmt = conn.createStatement();
            String tableName="creators";//hive表名
            sql = "select * from " + tableName+" limit 0,100";
            System.out.println("Running:" + sql);
            res = stmt.executeQuery(sql);
            System.out.println("执行 select * query 运行结果:");
            JSONArray jsonArray = org.angularbaby.test.JsonUtil.formatRsToJsonArray(res);

            return jsonArray.toString();
            //while (res.next()) {
            //System.out.println(res.getString(1) + "\t" + res.getString(1));
            //}

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        } finally {
            try {
                if(stmt!=null){
                    stmt.close();
                    stmt=null;
                }
                if (conn != null) {
                    conn.close();
                    conn = null;
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return "helloworld";
    }

    @RequestMapping("/directors")
    public String directors() {
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = getConn();
            System.out.println(conn);
            stmt = conn.createStatement();
            String tableName="directors";//hive表名
            sql = "select * from " + tableName+" limit 0,100";
            System.out.println("Running:" + sql);
            res = stmt.executeQuery(sql);
            System.out.println("执行 select * query 运行结果:");
            JSONArray jsonArray = org.angularbaby.test.JsonUtil.formatRsToJsonArray(res);

            return jsonArray.toString();
            //while (res.next()) {
            //System.out.println(res.getString(1) + "\t" + res.getString(1));
            //}

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        } finally {
            try {
                if(stmt!=null){
                    stmt.close();
                    stmt=null;
                }
                if (conn != null) {
                    conn.close();
                    conn = null;
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return "helloworld";
    }

    private static Connection getConn() throws ClassNotFoundException,
            SQLException {
        Class.forName(driverName);
        Connection conn = DriverManager.getConnection(url, user, password);
        return conn;
    }
}