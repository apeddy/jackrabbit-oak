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

import static java.lang.Math.min;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.S3Object;

public class ReverseFileReader implements Closeable {

    private static final int BUFFER_SIZE = 16 * 1024;

    private int bufferSize;

    private S3Object object;

    private byte[] buffer;

    private int bufferOffset;

    private int fileOffset;

    public ReverseFileReader(AwsContext directoryContext, String objectKey) throws IOException {
        try {
            this.object = directoryContext.s3.getObject(directoryContext.bucketName, objectKey);
            this.fileOffset = (int) object.getObjectMetadata().getContentLength();
        } catch (AmazonServiceException e) {
            if (e.getStatusCode() == 404) {
                this.fileOffset = 0;
            } else {
                throw new IOException(e);
            }
        }
        this.bufferSize = BUFFER_SIZE;
    }

    @Override
    public void close() throws IOException {
        if (object != null) {
            object.close();
            object = null;
        }
    }

    private void readBlock() throws IOException {
        if (buffer == null) {
            buffer = new byte[min(fileOffset, bufferSize)];
        } else if (fileOffset < buffer.length) {
            buffer = new byte[fileOffset];
        }

        if (buffer.length > 0) {
            fileOffset -= buffer.length;
            object.getObjectContent().read(buffer, fileOffset, buffer.length);
        }

        bufferOffset = buffer.length;
    }

    private String readUntilNewLine() {
        if (bufferOffset == -1) {
            return "";
        }
        int stop = bufferOffset;
        while (--bufferOffset >= 0) {
            if (buffer[bufferOffset] == '\n') {
                break;
            }
        }
        // bufferOffset points either the previous '\n' character or -1
        return new String(buffer, bufferOffset + 1, stop - bufferOffset - 1, Charset.defaultCharset());
    }

    public String readLine() throws IOException {
        if (bufferOffset == -1 && fileOffset == 0) {
            return null;
        }

        if (buffer == null) {
            readBlock();
        }

        List<String> result = new ArrayList<>(1);
        while (true) {
            result.add(readUntilNewLine());
            if (bufferOffset > -1) { // stopped on the '\n'
                break;
            }
            if (fileOffset == 0) { // reached the beginning of the file
                break;
            }
            readBlock();
        }
        Collections.reverse(result);
        return String.join("", result);
    }
}
