package org.xbib.elasticsearch.river.jdbc;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.testng.ITestContext;
import org.testng.ITestListener;
import org.testng.ITestResult;

public class Listener implements ITestListener {

    private final ESLogger logger = ESLoggerFactory.getLogger("test.Listener");

    @Override
    public void onTestStart(ITestResult result) {
        logger.info("----------------------------------------------------------");
        logger.info("starting {}", result.getMethod());
        logger.info("----------------------------------------------------------");
    }

    @Override
    public void onTestSuccess(ITestResult result) {
        logger.info("----------------------------------------------------------");
        logger.info("success {}", result.getMethod());
        logger.info("----------------------------------------------------------");
    }

    @Override
    public void onTestFailure(ITestResult result) {
        logger.info("----------------------------------------------------------");
        logger.info("failure of {}", result.getMethod());
        logger.info("----------------------------------------------------------");
    }

    @Override
    public void onTestSkipped(ITestResult result) {
        logger.info("skipped test {}", result.getMethod());
        result.setStatus(ITestResult.FAILURE);
    }

    @Override
    public void onTestFailedButWithinSuccessPercentage(ITestResult result) {
    }

    @Override
    public void onStart(ITestContext context) {
        logger.info("----------------------------------------------------------");
        logger.info("starting test {}", context.getName());
        logger.info("----------------------------------------------------------");
    }

    @Override
    public void onFinish(ITestContext context) {
        logger.info("----------------------------------------------------------");
        logger.info("finished test {}", context.getName());
        logger.info("----------------------------------------------------------");
    }

}

