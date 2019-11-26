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

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;

public final class DynamoDBMock {
    public static AmazonDynamoDB createClientWithTable(String tableName) {
        AmazonDynamoDB ddb = DynamoDBEmbedded.create().amazonDynamoDB();
        DynamoDB client = new DynamoDB(ddb);
        CreateTableRequest createTableRequest = new CreateTableRequest().withTableName(tableName)
                .withKeySchema(new KeySchemaElement(TABLE_ATTR_FILENAME, KeyType.HASH),
                        new KeySchemaElement(TABLE_ATTR_TIMESTAMP, KeyType.RANGE))
                .withAttributeDefinitions(new AttributeDefinition(TABLE_ATTR_FILENAME, ScalarAttributeType.S),
                        new AttributeDefinition(TABLE_ATTR_TIMESTAMP, ScalarAttributeType.N))
                .withProvisionedThroughput(new ProvisionedThroughput(1000L, 1500L));
        client.createTable(createTableRequest);
        return ddb;
    }
}