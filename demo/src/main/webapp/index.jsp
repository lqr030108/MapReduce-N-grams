<%@ page contentType="text/html; charset=UTF-8" pageEncoding="UTF-8" %>
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>MapReduce框架实现搜索联想和文本检索</title>
    <style type="text/css">
        /* 类选择器，设置input标签，text边框*/
        .userInput {
            width: 300px; /*框宽*/
            height: 25px; /*框高*/
            font-size: 20px; /*里面字大小*/
            padding-left: 5px; /*内补丁，距离框的距离*/
        }
        /*类选择器，设置div的样式*/
        .showDataDiv {
            width: 309px; /*宽度*/
            border: 1px solid lightgray; /*实线边框*/
            background-color: antiquewhite; /*设置背景颜色*/
            display: none; /*设置开始的div是隐藏的，不显示*/
        }
        .showSearchDiv{
            width: 600px; /*宽度*/
            border: 1px solid lightgray; /*实线边框*/
            background-color: #f8f4ed; /*设置背景颜色*/
            display: none; /*设置开始的div是隐藏的，不显示*/
        }
        /*设置p标签*/
        .showDataDiv p {
            padding-left: 5px; /*内补丁，距离框的距离*/
            margin-top: 2px; /*外补丁，p距离顶部的宽度*/
            margin-bottom: 2px; /*外补丁，p距离低部的宽度*/
        }
        /*p标签增加动作，点到其中一个选项变色并变成小手*/
        .showDataDiv p:hover {
            cursor: pointer; /*鼠标变成小手*/
            border: 1px blue solid; /*每选中一行，增加实线边框*/
            background-color: aliceblue; /*设置变换的背景色*/
        }
        .red {
            color: red;
        }
    </style>
</head>
<body>
<script type="text/javascript">
    window.onload = function() {
        document.getElementById("keywords").onkeyup = function(){
            if (this.value == "") {
                // 如果为空串，就把div隐藏起来
                // 不然查询联想之后，删除查询的内容，下面div还是保持原状
                document.getElementById("datadiv").style.display = "none"
            }else{
                // 发送ajax请求
                // 1. 创建AJAX核心对象
                var xmlHttpRequest = new XMLHttpRequest();
                // 2. 注册回调函数
                xmlHttpRequest.onreadystatechange = function() {
                    if (xmlHttpRequest.readyState == 4) {
                        if (xmlHttpRequest.status >= 200 && xmlHttpRequest.status < 300) {
                            var json = JSON.parse(xmlHttpRequest.responseText);
                            // 遍历数组
                            var html = ""
                            for (var i = 0; i < json.length; i++) {
                                // 点击p标签执行一个回调函数，把内容显示到文本框中
                                html += "<p onclick='setInput(\""+json[i].start_phrase + "\")'>" + json[i].start_phrase + "</p>"
                            }
                            // 让数据在div展示出来
                            document.getElementById("datadiv").innerHTML = html
                            // 让div显示出来
                            document.getElementById("datadiv").style.display = "block"
                        }
                    }
                }
                // 3. 开启通道，并把数据传过去，连接数据库进行模糊查询
                xmlHttpRequest.open("GET", "/demo_war_exploded/query?_="+new Date().getTime()+"&keywords=" + this.value, true)
                // 4. 发送请求
                xmlHttpRequest.send()
            }
        }
    }
    // 实现自动补全功能
    function setInput(content){
        // 先把数据显示到文本框中
        document.getElementById("keywords").value = content
        // 显示到文本框后，再次把div进行隐藏
        document.getElementById("datadiv").style.display = "none"
    }
    // 提交搜索表单
    function submitSearch() {
        var keywords = document.getElementById("keywords").value;
        // 发送ajax请求
        // 1. 创建AJAX核心对象
        var xmlHttpRequest = new XMLHttpRequest();
        // 2. 注册回调函数
        xmlHttpRequest.onreadystatechange = function() {
            if (xmlHttpRequest.readyState == 4) {
                if (xmlHttpRequest.status >= 200 && xmlHttpRequest.status < 300) {
                    var json = JSON.parse(xmlHttpRequest.responseText);
                    // 遍历数组
                    var result = ""
                    for (var i = 0; i < json.length; i++) {
                        // 点击p标签执行一个回调函数，把内容显示到文本框中
                        result += "<p><span class=\"red\">TEXT" + json[i].textID + "</span>: "+json[i].phrase1+"<span class=\"red\">"+json[i].keywords+"</span> "+json[i].phrase2+"</p>"
                    }
                    // 显示查询结果
                    document.getElementById("searchResult").innerHTML = result;
                    // 让div显示出来
                    document.getElementById("searchResult").style.display = "block"
                }
            }
        }
        // 3. 开启通道，并把数据传过去，连接数据库进行模糊查询
        xmlHttpRequest.open("GET", "/demo_war_exploded/search?keywords=" + keywords, true)
        // 4. 发送请求
        xmlHttpRequest.send()
    }
</script>
<h1 style="text-align: center">MapReduce框架实现搜索联想和文本检索</h1>
<!--文本框-->
<input type="text" class="userInput" id="keywords" style="display: block;margin:auto">
<button onclick="submitSearch()" style="display: block;margin:auto">Search</button>
<!--div盒子-->
<div id="datadiv" class="showDataDiv" style="display: block;margin:auto">
    <!--<p>北京疫情最新情况</p>
    <p>北京天气</p>
    <p>北京时间</p>
    <p>北京人</p>-->
</div>
<div id="searchResult" class="showSearchDiv" style="display: block;margin:auto"></div>
</body>
</html>