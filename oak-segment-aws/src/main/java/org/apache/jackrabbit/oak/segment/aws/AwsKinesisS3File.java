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
import java.util.Iterator;
import java.util.List;

import com.amazonaws.services.s3.model.S3ObjectSummary;

import org.apache.jackrabbit.oak.segment.spi.persistence.JournalFileReader;
import org.apache.jackrabbit.oak.segment.spi.persistence.JournalFileWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class AwsKinesisS3File {

    private static final Logger log = LoggerFactory.getLogger(AwsKinesisS3File.class);

    private final AwsContext directoryContext;
    private final String fileName;
    private final String streamName;

    public AwsKinesisS3File(AwsContext awsContext, String fileName, String streamName) {
        this.directoryContext = awsContext.withDirectory(fileName);
        this.fileName = fileName;
        this.streamName = streamName;
    }

    public String getName() {
        return fileName;
    }

    public JournalFileReader openJournalReader() throws IOException {
        return new AwsS3FolderAsFileReader(directoryContext, directoryContext.listObjects(""));
    }

    public JournalFileWriter openJournalWriter() {
        return new AwsKinesisToS3FileWriter(directoryContext, streamName);
    }

    public boolean exists() {
        try {
            return !directoryContext.listObjects("").isEmpty();
        } catch (IOException e) {
            log.error("Can't check if the file exists", e);
            return false;
        }
    }

    private static class AwsS3ObjectReader implements JournalFileReader {

        private final AwsContext directoryContext;
        private final S3ObjectSummary object;

        private ReverseFileReader reader;

        // private boolean metadataFetched;

        // private boolean firstLineReturned;

        private AwsS3ObjectReader(AwsContext directoryContext, S3ObjectSummary object) {
            this.directoryContext = directoryContext;
            this.object = object;
        }

        @Override
        public String readLine() throws IOException {
            if (reader == null) {
                // if (!metadataFetched) {
                //     Map<String, String> metadata = directoryContext.s3
                //             .getObjectMetadata(object.getBucketName(), object.getKey()).getUserMetadata();
                //     metadataFetched = true;
                //     if (metadata.containsKey("lastEntry")) {
                //         firstLineReturned = true;
                //         return metadata.get("lastEntry");
                //     }
                // }
                reader = new ReverseFileReader(directoryContext, object.getKey());
                // if (firstLineReturned) {
                //     while ("".equals(reader.readLine()))
                //         ; // the first line was already returned, let's fast-forward it
                // }
            }
            return reader.readLine();
        }

        @Override
        public void close() throws IOException {
        }
    }

    private static class AwsS3FolderAsFileReader implements JournalFileReader {

        private final Iterator<AwsS3ObjectReader> readers;

        private JournalFileReader currentReader;

        public AwsS3FolderAsFileReader(AwsContext directoryContext, List<S3ObjectSummary> objects) throws IOException {
            this.readers = objects.stream().sorted((a, b) -> {
                return b.getKey().compareTo(a.getKey());
            }).map(obj -> new AwsS3ObjectReader(directoryContext, obj)).iterator();
        }

        /**
         * Read the line from the journal, using LIFO strategy (last in, first out).
         * 
         * @return the journal record
         * @throws IOException
         */
        @Override
        public String readLine() throws IOException {
            String line;
            do {
                if (currentReader == null) {
                    if (!readers.hasNext()) {
                        return null;
                    }
                    currentReader = readers.next();
                }

                do {
                    line = currentReader.readLine();
                } while ("".equals(line));

                if (line == null) {
                    currentReader.close();
                    currentReader = null;
                }
            } while (line == null);

            return line;
        }

        @Override
        public void close() throws IOException {
            while (readers.hasNext()) {
                readers.next().close();
            }

            if (currentReader != null) {
                currentReader.close();
                currentReader = null;
            }
        }
    }

    private static class AwsKinesisToS3FileWriter implements JournalFileWriter {
        private final AwsContext directoryContext;
        private final String streamName;

        // TODO should we use custom partition keys
        private final String partitionKey = "default";

        public AwsKinesisToS3FileWriter(AwsContext directoryContext, String streamName) {
            this.directoryContext = directoryContext;
            this.streamName = streamName;
        }

        @Override
        public void close() {
            // Do nothing
        }

        @Override
        public void truncate() throws IOException {
            directoryContext.deleteAllObjects();
        }

        @Override
        public void writeLine(String line) throws IOException {
            directoryContext.putRecord(streamName, line + "\n");
        }
    }
}
