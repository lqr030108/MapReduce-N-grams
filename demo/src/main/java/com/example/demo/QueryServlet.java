package com.example.demo;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
@WebServlet("/query")
public class QueryServlet extends HttpServlet {
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        // 获取用户输入的关键字
        String keywords = request.getParameter("keywords");
        // jdbc代码连接数据库，根据关键字查询数据库，返回数据，拼接json格式的字符串
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        // 拼成JSON格式的字符串
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        try {
            // 注册驱动
            Class.forName("com.mysql.jdbc.Driver");
            // 获取连接
            String url = "jdbc:mysql://localhost:3306/associative-word?useUnicode=true&characterEncoding=UTF-8";
            String user = "root";
            String password = "liqirong2003";
            conn = DriverManager.getConnection(url, user, password);
            String sql = "select * from start_follow_words where start_phrase like ? order by count desc"; // 模糊查询的时候，条件不建议使用%开始，因为会让字段上的索引失效，查询效率降低。
            ps = conn.prepareStatement(sql);
            ps.setString(1, keywords + "%");
            rs = ps.executeQuery();
            // [{"content":"javascript"},{"content":"javaweb"},{"content":"java..."}]
            int counter = 0;
            while (rs.next() && counter < 10) {
                String start_phrase = rs.getString("start_phrase");
                String follow_words = rs.getString("follow_words");
                int count = rs.getInt("count");
                if (count > 20) {
                    sb.append("{\"start_phrase\":\"" + start_phrase + " " + follow_words + "\"},");
                    counter += 1;
                }
            }
        }catch(Exception e){
            e.printStackTrace();
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        // 最后会多一个逗号，进行截串
        response.setContentType("text/html;charset=UTF-8");
        response.getWriter().print(sb.subSequence(0, sb.length() - 1) + "]");
    }
}