/*
 * Copyright (C) 2015 Jörg Prante
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.xbib.elasticsearch.support;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.ITestContext;
import org.testng.ITestListener;
import org.testng.ITestResult;

public class TestListener implements ITestListener {

    private final Logger logger = LogManager.getLogger("test.Listener");

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
