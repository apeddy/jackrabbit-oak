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
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.ResponseMetadata;
import com.amazonaws.regions.Region;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.AddTagsToStreamRequest;
import com.amazonaws.services.kinesis.model.AddTagsToStreamResult;
import com.amazonaws.services.kinesis.model.CreateStreamRequest;
import com.amazonaws.services.kinesis.model.CreateStreamResult;
import com.amazonaws.services.kinesis.model.DecreaseStreamRetentionPeriodRequest;
import com.amazonaws.services.kinesis.model.DecreaseStreamRetentionPeriodResult;
import com.amazonaws.services.kinesis.model.DeleteStreamRequest;
import com.amazonaws.services.kinesis.model.DeleteStreamResult;
import com.amazonaws.services.kinesis.model.DeregisterStreamConsumerRequest;
import com.amazonaws.services.kinesis.model.DeregisterStreamConsumerResult;
import com.amazonaws.services.kinesis.model.DescribeLimitsRequest;
import com.amazonaws.services.kinesis.model.DescribeLimitsResult;
import com.amazonaws.services.kinesis.model.DescribeStreamConsumerRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamConsumerResult;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.DescribeStreamSummaryRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamSummaryResult;
import com.amazonaws.services.kinesis.model.DisableEnhancedMonitoringRequest;
import com.amazonaws.services.kinesis.model.DisableEnhancedMonitoringResult;
import com.amazonaws.services.kinesis.model.EnableEnhancedMonitoringRequest;
import com.amazonaws.services.kinesis.model.EnableEnhancedMonitoringResult;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.GetShardIteratorResult;
import com.amazonaws.services.kinesis.model.IncreaseStreamRetentionPeriodRequest;
import com.amazonaws.services.kinesis.model.IncreaseStreamRetentionPeriodResult;
import com.amazonaws.services.kinesis.model.ListShardsRequest;
import com.amazonaws.services.kinesis.model.ListShardsResult;
import com.amazonaws.services.kinesis.model.ListStreamConsumersRequest;
import com.amazonaws.services.kinesis.model.ListStreamConsumersResult;
import com.amazonaws.services.kinesis.model.ListStreamsRequest;
import com.amazonaws.services.kinesis.model.ListStreamsResult;
import com.amazonaws.services.kinesis.model.ListTagsForStreamRequest;
import com.amazonaws.services.kinesis.model.ListTagsForStreamResult;
import com.amazonaws.services.kinesis.model.MergeShardsRequest;
import com.amazonaws.services.kinesis.model.MergeShardsResult;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.RegisterStreamConsumerRequest;
import com.amazonaws.services.kinesis.model.RegisterStreamConsumerResult;
import com.amazonaws.services.kinesis.model.RemoveTagsFromStreamRequest;
import com.amazonaws.services.kinesis.model.RemoveTagsFromStreamResult;
import com.amazonaws.services.kinesis.model.SplitShardRequest;
import com.amazonaws.services.kinesis.model.SplitShardResult;
import com.amazonaws.services.kinesis.model.StartStreamEncryptionRequest;
import com.amazonaws.services.kinesis.model.StartStreamEncryptionResult;
import com.amazonaws.services.kinesis.model.StopStreamEncryptionRequest;
import com.amazonaws.services.kinesis.model.StopStreamEncryptionResult;
import com.amazonaws.services.kinesis.model.UpdateShardCountRequest;
import com.amazonaws.services.kinesis.model.UpdateShardCountResult;
import com.amazonaws.services.kinesis.waiters.AmazonKinesisWaiters;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;

public class MockAmazonKinesisClient implements AmazonKinesis {
    private final AmazonS3 s3;
    private final String bucketName;
    private final Map<String, String> map;
    private final String streamName = "gcjournalstream";

    private static int indexCounter = 1;
    private final int index;

    private MockAmazonKinesisClient(AmazonS3 s3, String bucketName, Map<String, String> map) {
        this.s3 = s3;
        this.bucketName = bucketName;
        this.map = map;
        this.index = indexCounter++;
    }

    public static MockAmazonKinesisClient createFirehose(AmazonS3 s3, String bucketName) {
        Map<String, String> map = new HashMap<>();
        map.put("journalstream", "journal.log");
        map.put("gclogstream", "gc.log");
        map.put("lockstream", "repo.lock");
        return new MockAmazonKinesisClient(s3, bucketName, map);
    }

    @Override
    public void setEndpoint(String endpoint) {
        // TODO Auto-generated method stub
    }

    @Override
    public void setRegion(Region region) {
        // TODO Auto-generated method stub
    }

    @Override
    public AddTagsToStreamResult addTagsToStream(AddTagsToStreamRequest addTagsToStreamRequest) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public CreateStreamResult createStream(CreateStreamRequest createStreamRequest) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public CreateStreamResult createStream(String streamName, Integer shardCount) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DecreaseStreamRetentionPeriodResult decreaseStreamRetentionPeriod(
            DecreaseStreamRetentionPeriodRequest decreaseStreamRetentionPeriodRequest) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DeleteStreamResult deleteStream(DeleteStreamRequest deleteStreamRequest) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DeleteStreamResult deleteStream(String streamName) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DeregisterStreamConsumerResult deregisterStreamConsumer(
            DeregisterStreamConsumerRequest deregisterStreamConsumerRequest) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DescribeLimitsResult describeLimits(DescribeLimitsRequest describeLimitsRequest) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DescribeStreamResult describeStream(DescribeStreamRequest describeStreamRequest) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DescribeStreamResult describeStream(String streamName) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DescribeStreamResult describeStream(String streamName, String exclusiveStartShardId) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DescribeStreamResult describeStream(String streamName, Integer limit, String exclusiveStartShardId) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DescribeStreamConsumerResult describeStreamConsumer(
            DescribeStreamConsumerRequest describeStreamConsumerRequest) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DescribeStreamSummaryResult describeStreamSummary(
            DescribeStreamSummaryRequest describeStreamSummaryRequest) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DisableEnhancedMonitoringResult disableEnhancedMonitoring(
            DisableEnhancedMonitoringRequest disableEnhancedMonitoringRequest) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public EnableEnhancedMonitoringResult enableEnhancedMonitoring(
            EnableEnhancedMonitoringRequest enableEnhancedMonitoringRequest) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public GetRecordsResult getRecords(GetRecordsRequest getRecordsRequest) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public GetShardIteratorResult getShardIterator(GetShardIteratorRequest getShardIteratorRequest) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public GetShardIteratorResult getShardIterator(String streamName, String shardId, String shardIteratorType) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public GetShardIteratorResult getShardIterator(String streamName, String shardId, String shardIteratorType,
            String startingSequenceNumber) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public IncreaseStreamRetentionPeriodResult increaseStreamRetentionPeriod(
            IncreaseStreamRetentionPeriodRequest increaseStreamRetentionPeriodRequest) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ListShardsResult listShards(ListShardsRequest listShardsRequest) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ListStreamConsumersResult listStreamConsumers(ListStreamConsumersRequest listStreamConsumersRequest) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ListStreamsResult listStreams(ListStreamsRequest listStreamsRequest) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ListStreamsResult listStreams() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ListStreamsResult listStreams(String exclusiveStartStreamName) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ListStreamsResult listStreams(Integer limit, String exclusiveStartStreamName) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ListTagsForStreamResult listTagsForStream(ListTagsForStreamRequest listTagsForStreamRequest) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public MergeShardsResult mergeShards(MergeShardsRequest mergeShardsRequest) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public MergeShardsResult mergeShards(String streamName, String shardToMerge, String adjacentShardToMerge) {
        // TODO Auto-generated method stub
        return null;
    }

    private SimpleDateFormat format1 = new SimpleDateFormat("yyyymmdd");
    private SimpleDateFormat format2 = new SimpleDateFormat("yyyy-mm-dd-hh-mm-ss");

    @Override
    public PutRecordResult putRecord(PutRecordRequest putRecordRequest) {
        Date now = new Date();
        String fileName = format1.format(now) + "-" + streamName + "-" + index + "-" + format2.format(now) + "-"
                + UUID.randomUUID().toString();

        InputStream input = new ByteArrayInputStream(putRecordRequest.getData().array());
        ObjectMetadata metadata = new ObjectMetadata();
        Map<String, String> userMetadata = new HashMap<>();
        userMetadata.put("shardId", UUID.randomUUID().toString());
        metadata.setUserMetadata(userMetadata);
        PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName,
                map.get(putRecordRequest.getStreamName()) + "/" + fileName, input, metadata);
        s3.putObject(putObjectRequest);
        PutRecordResult result = new PutRecordResult().withShardId(userMetadata.get("shardId"));
        return result;
    }

    @Override
    public PutRecordResult putRecord(String streamName, ByteBuffer data, String partitionKey) {
        PutRecordRequest request = new PutRecordRequest().withStreamName(streamName).withPartitionKey(partitionKey)
                .withData(data);
        return putRecord(request);
    }

    @Override
    public PutRecordResult putRecord(String streamName, ByteBuffer data, String partitionKey,
            String sequenceNumberForOrdering) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public PutRecordsResult putRecords(PutRecordsRequest putRecordsRequest) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public RegisterStreamConsumerResult registerStreamConsumer(
            RegisterStreamConsumerRequest registerStreamConsumerRequest) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public RemoveTagsFromStreamResult removeTagsFromStream(RemoveTagsFromStreamRequest removeTagsFromStreamRequest) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public SplitShardResult splitShard(SplitShardRequest splitShardRequest) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public SplitShardResult splitShard(String streamName, String shardToSplit, String newStartingHashKey) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public StartStreamEncryptionResult startStreamEncryption(
            StartStreamEncryptionRequest startStreamEncryptionRequest) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public StopStreamEncryptionResult stopStreamEncryption(StopStreamEncryptionRequest stopStreamEncryptionRequest) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public UpdateShardCountResult updateShardCount(UpdateShardCountRequest updateShardCountRequest) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void shutdown() {
        // TODO Auto-generated method stub
    }

    @Override
    public ResponseMetadata getCachedResponseMetadata(AmazonWebServiceRequest request) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public AmazonKinesisWaiters waiters() {
        // TODO Auto-generated method stub
        return null;
    }

}