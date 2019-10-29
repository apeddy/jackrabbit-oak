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

import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;

import static org.apache.jackrabbit.oak.segment.aws.Configuration.PID;

@ObjectClassDefinition(
        pid = {PID},
        name = "Apache Jackrabbit Oak AWS Segment Store Service",
        description = "AWS backend for the Oak Segment Node Store")
@interface Configuration {

    String PID = "org.apache.jackrabbit.oak.segment.aws.AwsSegmentStoreService";

    @AttributeDefinition(
            name = "AWS bucket name",
            description = "Name of the bucket to use. If it doesn't exists, it'll be created.")
    String bucketName();

    @AttributeDefinition(
            name = "AWS access key",
            description = "AWS access key which should be used to authenticate on the account")
    String accessKey();

    @AttributeDefinition(
            name = "AWS secret key",
            description = "Secret key which should be used to authenticate on the account")
    String secretKey();

    @AttributeDefinition(
            name = "Session token for temporary credentials (optional)",
            description = "Session token for temporary credentials which should be used to authenticate on the account")
    String sessionToken();

    @AttributeDefinition(
            name = "Root path",
            description = "Names of all the created blobs will be prefixed with this path")
    String rootPath() default AwsSegmentStoreService.DEFAULT_ROOT_PATH;

    @AttributeDefinition(
            name = "AWS region",
            description = "Name of the region containing the S3 bucke and Kinesis delivery streams")
    String region();

    @AttributeDefinition(
            name = "Stream name for gc.log",
            description = "Name of the Kinesis delivery stream used for gc.log")
    String gcLogStreamName();

    @AttributeDefinition(
            name = "Stream name for journal.log",
            description = "Name of the Kinesis delivery stream used for journal.log")
    String journalStreamName();

    @AttributeDefinition(
            name = "Stream name for repo.lock",
            description = "Name of the Kinesis delivery stream used for repo.lock")
    String lockStreamName();
}