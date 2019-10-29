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
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.AmazonKinesisException;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
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
import com.google.common.io.ByteStreams;

import org.apache.jackrabbit.oak.commons.Buffer;

public class AwsContext {
    public final AmazonS3 s3;
    public final String bucketName;
    public final String rootDirectory;

    public final AmazonKinesis kinesis;
    private final String gcLogStreamName;
    private final String journalStreamName;
    private final String lockStreamName;

    private AwsContext(AmazonS3 s3, String bucketName, String rootDirectory, AmazonKinesis kinesis,
            String gcLogStreamName, String journalStreamName, String lockStreamName) {
        this.s3 = s3;
        this.bucketName = bucketName;
        this.rootDirectory = rootDirectory.endsWith("/") ? rootDirectory : rootDirectory + "/";
        this.kinesis = kinesis;
        this.gcLogStreamName = gcLogStreamName;
        this.journalStreamName = journalStreamName;
        this.lockStreamName = lockStreamName;
    }

    public static AwsContext create(AmazonS3 s3, String bucketName, String rootDirectory, AmazonKinesis kinesis,
            String gcLogStream, String journalStream, String lockStream) {

        return new AwsContext(s3, bucketName, rootDirectory, kinesis, gcLogStream, journalStream, lockStream);
    }

    public static AwsContext create(AWSCredentialsProvider credentialsProvider, String bucketName, String rootDirectory,
            String region, String gcLogStream, String journalStream, String lockStream) {

        AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.DEFAULT_REGION)
                .withCredentials(credentialsProvider).build();
        AmazonKinesis kinesis = AmazonKinesisClientBuilder.standard().withRegion(region)
                .withCredentials(credentialsProvider).build();

        return AwsContext.create(s3, bucketName, rootDirectory, kinesis, gcLogStream, journalStream, lockStream);
    }

    public void putRecord(String streamName, String content) throws IOException {
        PutRecordRequest putRecordRequest = new PutRecordRequest().withStreamName(streamName)
                .withPartitionKey("default").withData(ByteBuffer.wrap(content.getBytes()));
        try {
            kinesis.putRecord(putRecordRequest);
        } catch (AmazonKinesisException e) {
            throw new IOException(e);
        }
    }

    public List<Record> getRecordsFromTimestamp(String streamName, String shardId, Date timestamp) throws IOException {
        try {
            GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest().withStreamName(streamName)
                    .withShardIteratorType(ShardIteratorType.AT_TIMESTAMP).withTimestamp(timestamp)
                    .withShardId(shardId);
            String shardIterator = kinesis.getShardIterator(getShardIteratorRequest).getShardIterator();
            GetRecordsRequest getRecordsRequest = new GetRecordsRequest().withShardIterator(shardIterator);
            List<Record> result = kinesis.getRecords(getRecordsRequest).getRecords();
            return result;
        } catch (AmazonKinesisException e) {
            throw new IOException(e);
        }
    }

    public void copyObject(String objectKey) throws IOException {
        String newObjectKey = rootDirectory + Paths.get(objectKey).getFileName();
        try {
            s3.copyObject(new CopyObjectRequest(bucketName, objectKey, bucketName, newObjectKey));
        } catch (AmazonServiceException e) {
            throw new IOException(e);
        }
    }

    public void deleteAllObjects() throws IOException {
        List<KeyVersion> keys = listObjects("").stream().map(i -> new KeyVersion(i.getKey()))
                .collect(Collectors.toList());
        if (keys.size() > 0) {
            try {
                DeleteObjectsRequest request = new DeleteObjectsRequest(bucketName).withKeys(keys);
                s3.deleteObjects(request);
            } catch (AmazonServiceException e) {
                throw new IOException(e);
            }
        }
    }

    public boolean doesObjectExist(String name) throws IOException {
        try {
            return s3.doesObjectExist(bucketName, rootDirectory + name);
        } catch (AmazonServiceException e) {
            throw new IOException(e);
        }
    }

    public String getPath() {
        return rootDirectory;
    }

    public List<String> listPrefixes() throws IOException {
        return listObjectsInternal("").getCommonPrefixes();
    }

    public List<S3ObjectSummary> listObjects(String prefix) throws IOException {
        return listObjectsInternal(prefix).getObjectSummaries();
    }

    public Buffer readObject(String name) throws IOException {
        return readObject(name, false);
    }

    public Buffer readObject(String name, boolean offHeap) throws IOException {
        GetObjectRequest request = new GetObjectRequest(bucketName, rootDirectory + name);

        try (S3Object object = s3.getObject(request)) {
            byte[] data = ByteStreams.toByteArray(object.getObjectContent());

            Buffer buffer;
            if (offHeap) {
                buffer = Buffer.allocateDirect(data.length);
            } else {
                buffer = Buffer.allocate(data.length);
            }

            buffer.put(data);

            return buffer;
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

    public AwsContext withDirectory(String childDirectory) {
        return new AwsContext(s3, bucketName, rootDirectory + childDirectory, kinesis, gcLogStreamName,
                journalStreamName, lockStreamName);
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

    public String getGCLogStreamName() {
        return gcLogStreamName;
    }

    public String getJournalStreamName() {
        return journalStreamName;
    }

    public String getLockStreamName() {
        return lockStreamName;
    }
}
