package org.elasticsearch.river.jdbc.strategy;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.testng.ITestContext;
import org.testng.ITestListener;
import org.testng.ITestResult;

public class Listener implements ITestListener {

    private final ESLogger logger = ESLoggerFactory.getLogger(Listener.class.getName());
    
    @Override
    public void onTestStart(ITestResult result) {
        logger.info("------------------------------------------------------");
        logger.info("Starting test method: {}", result.getName());
    }

    @Override
    public void onTestSuccess(ITestResult result) {
        // TODO Auto-generated method stub

    }

    @Override
    public void onTestFailure(ITestResult result) {
        // TODO Auto-generated method stub

    }

    @Override
    public void onTestSkipped(ITestResult result) {
        // TODO Auto-generated method stub
        logger.info("Skipped test: {}", result.getMethod().getMethodName());
        result.setStatus(ITestResult.FAILURE);
    }

    @Override
    public void onTestFailedButWithinSuccessPercentage(ITestResult result) {
        // TODO Auto-generated method stub

    }

    @Override
    public void onStart(ITestContext context) {
        logger.info("------------------------------------------------------");
        logger.info("Starting test: {}", context.getName());    
    }

    @Override
    public void onFinish(ITestContext context) {
        // TODO Auto-generated method stub

    }

}
