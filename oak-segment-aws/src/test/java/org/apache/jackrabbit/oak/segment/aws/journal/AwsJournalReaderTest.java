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
package org.apache.jackrabbit.oak.segment.aws.journal;

import java.io.IOException;
import java.util.Date;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;

import org.apache.jackrabbit.oak.segment.aws.AwsContext;
import org.apache.jackrabbit.oak.segment.aws.AwsJournalFile;
import org.apache.jackrabbit.oak.segment.aws.DynamoDBMock;
import org.apache.jackrabbit.oak.segment.file.JournalReader;
import org.apache.jackrabbit.oak.segment.file.JournalReaderTest;
import org.junit.Before;

public class AwsJournalReaderTest extends JournalReaderTest {

    private AwsContext awsContext;

    @Before
    public void setup() {
        String tableName = "oak-" + new Date().getTime();
        String lockTableName = "locktable-" + new Date().getTime();
        AmazonDynamoDB ddb = DynamoDBMock.createClient(tableName, lockTableName);
        awsContext = AwsContext.create(null, null, "oak", ddb, tableName, lockTableName);
    }

    protected JournalReader createJournalReader(String s) throws IOException {
        String fileName = "journal.log";
        for (String line : s.split("\n")) {
            if (line != null && !line.isEmpty()) {
                awsContext.putDocument(fileName, line);
            }
        }
        return new JournalReader(new AwsJournalFile(awsContext, fileName));
    }
}
