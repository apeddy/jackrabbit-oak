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

import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

import org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class AwsContext {
    public static final String INDEX_NAME = "idx_oak_filename";

    public static final String TABLE_ATTR_TIMESTAMP = "timestamp";

    public static final String TABLE_ATTR_FILENAME = "filename";

    private static final String TABLE_ATTR_CONTENT = "content";

    private static final Logger log = LoggerFactory.getLogger(AwsContext.class);

    private final AmazonS3 s3;
    private final String bucketName;
    private final String rootDirectory;

    private final Table table;

    private AwsContext(AmazonS3 s3, String bucketName, String rootDirectory, AmazonDynamoDB ddb, String tableName) {
        this.s3 = s3;
        this.bucketName = bucketName;
        this.rootDirectory = rootDirectory.endsWith("/") ? rootDirectory : rootDirectory + "/";
        DynamoDB dynamoDB = new DynamoDB(ddb);
        this.table = dynamoDB.getTable(tableName);
    }

    public static AwsContext create(AmazonS3 s3, String bucketName, String rootDirectory, AmazonDynamoDB ddb,
            String tableName) {

        return new AwsContext(s3, bucketName, rootDirectory, ddb, tableName);
    }

    public boolean doesObjectExist(String name) {
        try {
            return s3.doesObjectExist(bucketName, rootDirectory + name);
        } catch (AmazonServiceException e) {
            log.error("Can't check if the manifest exists", e);
            return false;
        }
    }

    public S3Object getObject(String name) throws IOException {
        try {
            GetObjectRequest request = new GetObjectRequest(bucketName, rootDirectory + name);
            return s3.getObject(request);
        } catch (AmazonServiceException e) {
            throw new IOException(e);
        }
    }

    public void putObject(String name, InputStream input) throws IOException {
        try {
            PutObjectRequest request = new PutObjectRequest(bucketName, rootDirectory + name, input,
                    new ObjectMetadata());
            s3.putObject(request);
        } catch (AmazonServiceException e) {
            throw new IOException(e);
        }
    }

    public void deleteAllDocuments(String fileName) {
        throw new NotImplementedException("message");
    }

    public List<String> getDocumentContents(String fileName) throws IOException {
        QuerySpec spec = new QuerySpec().withKeyConditionExpression(TABLE_ATTR_FILENAME + " = :v_id")
                .withValueMap(new ValueMap().withString(":v_id", fileName));
        try {
            ItemCollection<QueryOutcome> outcome = table.query(spec);
            return StreamSupport.stream(outcome.spliterator(), false).map(item -> item.getString(TABLE_ATTR_CONTENT))
                    .collect(Collectors.toList());
        } catch (AmazonDynamoDBException e) {
            throw new IOException(e);
        }
    }

    public void putDocument(String fileName, String line) throws IOException {
        Item item = new Item().with(TABLE_ATTR_TIMESTAMP, new Date().getTime()).with(TABLE_ATTR_FILENAME, fileName)
                .with(TABLE_ATTR_CONTENT, line);
        try {
            table.putItem(item);
        } catch (AmazonDynamoDBException e) {
            throw new IOException(e);
        }
    }
}
