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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.Date;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;

import org.apache.jackrabbit.oak.segment.spi.persistence.JournalFileReader;
import org.apache.jackrabbit.oak.segment.spi.persistence.JournalFileWriter;
import org.junit.Before;
import org.junit.Test;

public class AwsJournalFileTest {

    private AwsJournalFile journal;

    @Before
    public void setup() throws InvalidKeyException, URISyntaxException {
        String bucketName = "oak-test";
        String tableName = "testtable-" + new Date().getTime();
        String lockTableName = "locktable-" + new Date().getTime();
        AmazonDynamoDB ddb = DynamoDBMock.createClient(tableName, lockTableName);
        AwsContext awsContext = AwsContext.create(null, bucketName, "oak", ddb, tableName, lockTableName);
        journal = new AwsJournalFile(awsContext, "journal.log");
    }

    @Test
    public void testReadFromJournalFile() throws IOException, InterruptedException {
        assertFalse(journal.exists());

        JournalFileWriter writer = journal.openJournalWriter();
        for (int i = 0; i < 100; i++) {
            writer.writeLine("line " + i);
        }

        JournalFileReader reader = journal.openJournalReader();

        for (int i = 99; i >= 0; i--) {
            assertEquals("line " + i, reader.readLine());
        }
    }

    @Test
    public void testTruncateJournalFile() throws IOException {
        assertFalse(journal.exists());

        JournalFileWriter writer = journal.openJournalWriter();
        for (int i = 0; i < 100; i++) {
            writer.writeLine("line " + i);
        }

        assertTrue(journal.exists());

        writer.truncate();

        assertFalse(journal.exists());
    }
}
