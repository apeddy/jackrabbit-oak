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

import java.util.UUID;

import com.adobe.testing.s3mock.junit4.S3MockRule;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.s3.AmazonS3;

import org.apache.jackrabbit.oak.segment.file.tar.TarFiles;
import org.apache.jackrabbit.oak.segment.file.tar.TarFilesTest;
import org.apache.jackrabbit.oak.segment.spi.monitor.FileStoreMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.monitor.RemoteStoreMonitorAdapter;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

public class AzureTarFilesTest extends TarFilesTest {

    @ClassRule
    public static final S3MockRule S3_MOCK_RULE = S3MockRule.builder().silent().build();

    private AmazonS3 s3;
    private AmazonKinesis kinesis;

    @Before
    @Override
    public void setUp() throws Exception {
        String bucketName = "bkt-" + UUID.randomUUID().toString();

        s3 = S3_MOCK_RULE.createS3Client();
        s3.createBucket(bucketName);
        kinesis = MockAmazonKinesisClient.createFirehose(s3, bucketName);

        AwsContext awsContext = AwsContext.create(s3, bucketName, "oak", kinesis, "gclogstream", "journalstream",
                "lockstream");
        tarFiles = TarFiles.builder().withDirectory(folder.newFolder()).withTarRecovery((id, data, recovery) -> {
            // Intentionally left blank
        }).withIOMonitor(new IOMonitorAdapter()).withFileStoreMonitor(new FileStoreMonitorAdapter())
                .withRemoteStoreMonitor(new RemoteStoreMonitorAdapter()).withMaxFileSize(MAX_FILE_SIZE)
                .withPersistence(new AwsPersistence(awsContext)).build();
    }

    @Test
    @Override
    @Ignore
    public void testGetGraph() throws Exception {
        super.testGetGraph();
    }

    @Test
    @Override
    @Ignore
    public void testCleanupConnectedSegments() throws Exception {
        super.testCleanupConnectedSegments();
    }

    @Test
    @Override
    @Ignore
    public void testReadSegmentFromReader() throws Exception {
        super.testReadSegmentFromReader();
    }

    @Test
    @Override
    @Ignore
    public void testCleanup() throws Exception {
        super.testCleanup();
    }
}
