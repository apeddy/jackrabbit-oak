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

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import org.junit.rules.ExternalResource;

import io.findify.s3mock.S3Mock;

public class S3MockRule extends ExternalResource {
    S3Mock api = new S3Mock.Builder().withPort(8001).withInMemoryBackend().build();

    public AmazonS3 createClient() {
        EndpointConfiguration endpoint = new EndpointConfiguration("http://localhost:8001", "us-west-2");
        AmazonS3 client = AmazonS3ClientBuilder.standard().withPathStyleAccessEnabled(true)
                .withEndpointConfiguration(endpoint)
                .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials())).build();
        return client;
    }

    @Override
    protected void before() {
        api.start();
    }

    @Override
    protected void after() {
        api.stop();
    }
}
