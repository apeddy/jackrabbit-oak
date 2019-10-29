/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.segment.aws;

import org.apache.jackrabbit.oak.segment.spi.persistence.RepositoryLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class AwsRepositoryLock implements RepositoryLock {

    private static final Logger log = LoggerFactory.getLogger(AwsRepositoryLock.class);

    private static final int TIMEOUT_SEC = Integer.getInteger("oak.segment.aws.lock.timeout", 0);

    private static int INTERVAL = 60;

    private final Runnable shutdownHook;

    private final AwsContext awsContext;

    private final String lockName;

    private final ExecutorService executor;

    private final int timeoutSec;

    private String leaseId;

    private volatile boolean doUpdate;

    public AwsRepositoryLock(AwsContext awsContext, String lockName, Runnable shutdownHook) {
        this(awsContext, lockName, shutdownHook, TIMEOUT_SEC);
    }

    public AwsRepositoryLock(AwsContext awsContext, String lockName, Runnable shutdownHook, int timeoutSec) {
        this.shutdownHook = shutdownHook;
        this.awsContext = awsContext;
        this.lockName = lockName;
        this.executor = Executors.newSingleThreadExecutor();
        this.timeoutSec = timeoutSec;
    }

    public AwsRepositoryLock lock() throws IOException {
        long start = System.currentTimeMillis();
        Exception ex = null;
        do {
            leaseId = UUID.randomUUID().toString();
            // try {
            //     blob.openOutputStream().close();
            //     leaseId = blob.acquireLease(INTERVAL, null);
            //     log.info("Acquired lease {}", leaseId);
            // } catch (IOException e) {
            //     if (ex == null) {
            //         log.info("Can't acquire the lease. Retrying every 1s. Timeout is set to {}s.", timeoutSec);
            //     }
            //     ex = e;
            //     if ((System.currentTimeMillis() - start) / 1000 < timeoutSec) {
            //         try {
            //             Thread.sleep(1000);
            //         } catch (InterruptedException e1) {
            //             throw e;
            //         }
            //     } else {
            //         break;
            //     }
            // }
        } while (leaseId == null);
        if (leaseId == null) {
            log.error("Can't acquire the lease in {}s.", timeoutSec);
            throw new IOException(ex);
        } else {
            executor.submit(this::refreshLease);
            return this;
        }
    }

    private void refreshLease() {
        doUpdate = true;
        long lastUpdate = 0;
        while (doUpdate) {
            // try {
            //     long timeSinceLastUpdate = (System.currentTimeMillis() - lastUpdate) / 1000;
            //     if (timeSinceLastUpdate > INTERVAL / 2) {
            //         blob.renewLease(AccessCondition.generateLeaseCondition(leaseId));
            //         lastUpdate = System.currentTimeMillis();
            //     }
            // } catch (IOException e) {
            //     log.error("Can't renew the lease", e);
            //     shutdownHook.run();
            //     doUpdate = false;
            //     return;
            // }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                log.error("Interrupted the lease renewal loop", e);
            }
        }
    }

    @Override
    public void unlock() throws IOException {
        doUpdate = false;
        executor.shutdown();
        try {
            executor.awaitTermination(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new IOException(e);
        } finally {
            releaseLease();
        }
    }

    private void releaseLease() throws IOException {
        // blob.releaseLease(AccessCondition.generateLeaseCondition(leaseId));
        // blob.delete();
        log.info("Released lease {}", leaseId);
        leaseId = null;
    }
}
