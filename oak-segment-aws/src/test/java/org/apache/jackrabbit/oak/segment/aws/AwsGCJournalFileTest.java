/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.segment.aws;

import static org.apache.jackrabbit.oak.segment.aws.AwsContext.TABLE_ATTR_FILENAME;
import static org.apache.jackrabbit.oak.segment.aws.AwsContext.TABLE_ATTR_TIMESTAMP;

import java.io.IOException;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.jackrabbit.oak.segment.file.GcJournalTest;
import org.apache.jackrabbit.oak.segment.spi.monitor.FileStoreMonitor;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitor;
import org.apache.jackrabbit.oak.segment.spi.monitor.RemoteStoreMonitor;
import org.apache.jackrabbit.oak.segment.spi.persistence.GCJournalFile;
import org.apache.jackrabbit.oak.segment.spi.persistence.JournalFile;
import org.apache.jackrabbit.oak.segment.spi.persistence.ManifestFile;
import org.apache.jackrabbit.oak.segment.spi.persistence.RepositoryLock;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveManager;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class AwsGCJournalFileTest extends GcJournalTest {

    private AwsContext awsContext;

    @Before
    public void setup() {
        String bucketName = "testbkt1";
        AmazonDynamoDB ddb = DynamoDBEmbedded.create().amazonDynamoDB();
        DynamoDB client = new DynamoDB(ddb);
        String tableName = "testtable";

        CreateTableRequest createTableRequest = new CreateTableRequest().withTableName(tableName)
                .withKeySchema(new KeySchemaElement(TABLE_ATTR_FILENAME, KeyType.HASH),
                        new KeySchemaElement(TABLE_ATTR_TIMESTAMP, KeyType.RANGE))
                .withAttributeDefinitions(new AttributeDefinition(TABLE_ATTR_FILENAME, ScalarAttributeType.S),
                        new AttributeDefinition(TABLE_ATTR_TIMESTAMP, ScalarAttributeType.N))
                .withProvisionedThroughput(new ProvisionedThroughput(1000L, 1500L));
        client.createTable(createTableRequest);

        awsContext = AwsContext.create(null, bucketName, "oak", ddb, tableName);
    }

    @Override
    protected SegmentNodeStorePersistence getPersistence() throws Exception {
        return new MockPersistence(awsContext);
    }

    @Test
    @Ignore
    @Override
    public void testReadOak16GCLog() throws Exception {
        super.testReadOak16GCLog();
    }

    @Test
    @Ignore
    @Override
    public void testUpdateOak16GCLog() throws Exception {
        super.testUpdateOak16GCLog();
    }

    private static class MockPersistence implements SegmentNodeStorePersistence {

        private final AwsContext awsContext;

        public MockPersistence(AwsContext awsContext) {
            this.awsContext = awsContext;
        }

        @Override
        public SegmentArchiveManager createArchiveManager(boolean arg0, boolean arg1, IOMonitor arg2,
                FileStoreMonitor arg3, RemoteStoreMonitor arg4) throws IOException {
            throw new IOException();
        }

        @Override
        public GCJournalFile getGCJournalFile() throws IOException {
            return new AwsGCJournalFile(awsContext, "gc.log");
        }

        @Override
        public JournalFile getJournalFile() {
            return null;
        }

        @Override
        public ManifestFile getManifestFile() throws IOException {
            throw new IOException();
        }

        @Override
        public RepositoryLock lockRepository() throws IOException {
            throw new IOException();
        }

        @Override
        public boolean segmentFilesExist() {
            throw new NotImplementedException("message");
        }

    }
}
