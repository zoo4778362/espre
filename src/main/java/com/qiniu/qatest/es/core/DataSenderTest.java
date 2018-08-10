package com.qiniu.qatest.es.core;

import com.qiniu.pandora.pipeline.error.SendPointError;
import com.qiniu.pandora.pipeline.points.Point;
import com.qiniu.pandora.pipeline.sender.ParallelDataSender;
import com.qiniu.pandora.pipeline.sender.Sender;
import com.qiniu.pandora.util.Auth;
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;

import java.io.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class DataSenderTest extends AbstractJavaSamplerClient {

    private String ak;
    private String sk;
    private String repoName;
    private Auth auth;
    private Sender sender;
    private String type;
    private String path;//数据点读取路径
    private String pathbigger;
    private String patherror;
    private String pipelineHost;

    @Override
    public void setupTest(JavaSamplerContext context) {
        ak = context.getParameter("ak");
        sk = context.getParameter("sk");
        repoName = context.getParameter("reponame");
        type = context.getParameter("type");
        path = context.getParameter("path");
        pathbigger = context.getParameter("pathbigger");
        patherror = context.getParameter("patherror");
        pipelineHost = context.getParameter("pipelinehost");
        auth = Auth.create(ak, sk);
        sender = new ParallelDataSender(repoName, this.auth,pipelineHost ,5);
    }

    public SampleResult runTest(JavaSamplerContext javaSamplerContext) {
        SampleResult sr = new SampleResult();
        try {
            //记入开始时间
            sr.sampleStart();
            qiniuSender();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            //记录结束时间
            sr.sampleEnd();
            this.sender.close();
        }
        return sr;
    }

    @Override
    public void teardownTest(JavaSamplerContext context) {
        super.teardownTest(context);
    }


    @Override
    public Arguments getDefaultParameters() {
        Arguments params = new Arguments();
        params.addArgument("ak", "七牛ak");
        params.addArgument("sk", "七牛sk");
        params.addArgument("reponame", "七牛logdb仓库名");
        params.addArgument("type", "打点方式");//支持根据type选择打点方式  1.默认数据 2.文件读取正常数据 3.文件写入大数据 4.文件写入错误数据
        params.addArgument("path", "文件读取路径");
        params.addArgument("pathbigger", "大数据文件读取路径");//多次引用会有冲突
        params.addArgument("patherror", "错误文件读取路径");
        params.addArgument("pipelinehost","打点路由地址");
        return params;
    }

    private List<Point> createDate() {
        int sendertype = Integer.parseInt(this.type);
        switch (sendertype) {
            case 1:
                return defalutData();
            case 2:
                return pointsFromFile(this.path);
            case 3:
                return pointsFromFile(this.pathbigger);
            case 4:
                return pointsFromFile(this.patherror);
            default:
                break;
        }
        return new ArrayList<Point>();
    }

    /**
     * type 1的默认数据。100个点
     *
     * @return
     */
    private List<Point> defalutData() {
        List<Point> points = new ArrayList<Point>();
        for (int i = 0; i < 100; i++) {
            points.add(makePoint(i));
        }
        return points;
    }

    /**
     * 发送数据点
     */
    private void qiniuSender() {
        SendPointError error = this.sender.send(createDate());
        System.out.println(error.getExceptions());
    }

    private Point makePoint(int i) {
        Point p = new Point();
        p.append("tag1", i % 3);
        p.append("tag2", String.format("tag%s", i % 3));
        p.append("l1", i);
        p.append("f1", 4.5 + i);
        p.append("t", new Date());
        return p;
    }

    /**
     * 根据path读取文件，转化为point点，注意文件格式
     *
     * @return
     */
    private List<Point> pointsFromFile(String path) {
        File file = new File(path);
        Point p;
        BufferedReader reader = null;
        String temp;
        List<Point> points = new ArrayList<Point>();
        if ( !file.exists() || null == file) {
            return points;
        }
        int line = 1;
        try {
            reader = new BufferedReader(new FileReader(file));
            while ((temp = reader.readLine()) != null) {
                p = Point.fromPointString(temp);
                points.add(p);
                line++;
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (null != reader) {
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return points;
    }
}
