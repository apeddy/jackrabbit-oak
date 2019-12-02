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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import org.apache.jackrabbit.oak.commons.Buffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class AwsContext {
    public static final String INDEX_NAME = "idx_oak_filename";

    public static final String TABLE_ATTR_TIMESTAMP = "timestamp";

    public static final String TABLE_ATTR_FILENAME = "filename";

    public static final String TABLE_ATTR_CONTENT = "content";

    private static final Logger log = LoggerFactory.getLogger(AwsContext.class);

    private static final int TABLE_MAX_BATCH_WRITE_SIZE = 25;

    private final AmazonS3 s3;
    private final String bucketName;
    private final String rootDirectory;

    public final AmazonDynamoDB ddb;
    private final Table table;
    private final String tableName;
    public final String lockTableName;

    private AwsContext(AmazonS3 s3, String bucketName, String rootDirectory, AmazonDynamoDB ddb, String tableName,
            String lockTableName) {
        this.s3 = s3;
        this.bucketName = bucketName;
        this.rootDirectory = rootDirectory.endsWith("/") ? rootDirectory : rootDirectory + "/";
        this.ddb = ddb;
        this.table = new DynamoDB(ddb).getTable(tableName);
        this.tableName = tableName;
        this.lockTableName = lockTableName;
    }

    public static AwsContext create(AmazonS3 s3, String bucketName, String rootDirectory, AmazonDynamoDB ddb,
            String tableName, String lockTableName) {

        return new AwsContext(s3, bucketName, rootDirectory, ddb, tableName, lockTableName);
    }

    public AwsContext withDirectory(String childDirectory) {
        return new AwsContext(s3, bucketName, rootDirectory + childDirectory, ddb, tableName, lockTableName);
    }

    public String getPath() {
        return rootDirectory;
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

    public ObjectMetadata getObjectMetadata(String key) {
        return s3.getObjectMetadata(bucketName, key);
    }

    public Buffer readObjectToBuffer(String name, boolean offHeap) throws IOException {
        byte[] data = readObject(rootDirectory + name);
        Buffer buffer = offHeap ? Buffer.allocateDirect(data.length) : Buffer.allocate(data.length);
        buffer.put(data);
        buffer.flip();
        return buffer;
    }

    public byte[] readObject(String key) throws IOException {
        try (S3Object object = s3.getObject(bucketName, key)) {
            int length = (int) object.getObjectMetadata().getContentLength();
            byte[] data = new byte[length];
            if (length > 0) {
                try (InputStream stream = object.getObjectContent()) {
                    stream.read(data, 0, length);
                }
            }
            return data;
        } catch (AmazonServiceException e) {
            throw new IOException(e);
        }
    }

    public void writeObject(String name, byte[] data) throws IOException {
        writeObject(name, data, new HashMap<>());
    }

    public void writeObject(String name, byte[] data, Map<String, String> userMetadata) throws IOException {
        InputStream input = new ByteArrayInputStream(data);
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setUserMetadata(userMetadata);
        PutObjectRequest request = new PutObjectRequest(bucketName, rootDirectory + name, input, metadata);
        try {
            s3.putObject(request);
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

    public void copyObject(String objectKey) throws IOException {
        // TODO could cause problems
        String newObjectKey = rootDirectory + Paths.get(objectKey).getFileName().toString();
        try {
            s3.copyObject(new CopyObjectRequest(bucketName, objectKey, bucketName, newObjectKey));
        } catch (AmazonServiceException e) {
            throw new IOException(e);
        }
    }

    public boolean deleteAllObjects() {
        try {
            List<KeyVersion> keys = listObjects("").stream().map(i -> new KeyVersion(i.getKey()))
                    .collect(Collectors.toList());
            DeleteObjectsRequest request = new DeleteObjectsRequest(bucketName).withKeys(keys);
            s3.deleteObjects(request);
            return true;
        } catch (AmazonServiceException | IOException e) {
            log.error("Can't delete objects from {}", rootDirectory, e);
            return false;
        }
    }

    public List<String> listPrefixes() throws IOException {
        return listObjectsInternal("").getCommonPrefixes();
    }

    public List<S3ObjectSummary> listObjects(String prefix) throws IOException {
        return listObjectsInternal(prefix).getObjectSummaries();
    }

    private ListObjectsV2Result listObjectsInternal(String prefix) throws IOException {
        ListObjectsV2Request request = new ListObjectsV2Request().withBucketName(bucketName)
                .withPrefix(rootDirectory + prefix).withDelimiter("/");
        try {
            return s3.listObjectsV2(request);
        } catch (AmazonServiceException e) {
            throw new IOException(e);
        }
    }

    public void deleteAllDocuments(String fileName) throws IOException {
        List<PrimaryKey> primaryKeys = getDocumentsStream(fileName).map(item -> {
            return new PrimaryKey(TABLE_ATTR_FILENAME, item.getString(TABLE_ATTR_FILENAME), TABLE_ATTR_TIMESTAMP,
                    item.getNumber(TABLE_ATTR_TIMESTAMP));
        }).collect(Collectors.toList());

        for (int i = 0; i < primaryKeys.size(); i += TABLE_MAX_BATCH_WRITE_SIZE) {
            PrimaryKey[] currentKeys = new PrimaryKey[Math.min(TABLE_MAX_BATCH_WRITE_SIZE, primaryKeys.size() - i)];
            for (int j = 0; j < currentKeys.length; j++) {
                currentKeys[j] = primaryKeys.get(i + j);
            }

            new DynamoDB(ddb).batchWriteItem(new TableWriteItems(tableName).withPrimaryKeysToDelete(currentKeys));
        }
    }

    public Stream<Item> getDocumentsStream(String fileName) throws IOException {
        String FILENAME_KEY = ":v_filename";
        QuerySpec spec = new QuerySpec().withScanIndexForward(false)
                .withKeyConditionExpression(TABLE_ATTR_FILENAME + " = " + FILENAME_KEY)
                .withValueMap(new ValueMap().withString(FILENAME_KEY, fileName));
        try {
            ItemCollection<QueryOutcome> outcome = table.query(spec);
            return StreamSupport.stream(outcome.spliterator(), false);
        } catch (AmazonDynamoDBException e) {
            throw new IOException(e);
        }
    }

    public List<String> getDocumentContents(String fileName) throws IOException {
        return getDocumentsStream(fileName).map(item -> item.getString(TABLE_ATTR_CONTENT))
                .collect(Collectors.toList());
    }

    public void putDocument(String fileName, String line) throws IOException {
        try {
            Thread.sleep(1L);
        } catch (InterruptedException e) {
        }
        Item item = new Item().with(TABLE_ATTR_TIMESTAMP, new Date().getTime()).with(TABLE_ATTR_FILENAME, fileName)
                .with(TABLE_ATTR_CONTENT, line);
        try {
            table.putItem(item);
        } catch (AmazonDynamoDBException e) {
            throw new IOException(e);
        }
    }
}
