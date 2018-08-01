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

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class DataSenderTest extends AbstractJavaSamplerClient {

    private String ak;
    private String sk;
    private String repoName;
    private Auth auth;
    private Sender sender;

    @Override
    public void setupTest(JavaSamplerContext context) {
        ak = context.getParameter("ak");
        sk = context.getParameter("sk");
        repoName = context.getParameter("reponame");
        auth = Auth.create(ak, sk);
        sender = new ParallelDataSender(repoName, this.auth, 5);
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
        params.addArgument("ak", "null");
        params.addArgument("sk", "null");
        params.addArgument("reponame", null);
        return params;
    }

    private void qiniuSender() {
        //5个线程同时打点，每次打点数为100
        List<Point> points = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            points.add(makePoint(i));
        }
        SendPointError error = this.sender.send(points);
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
}
