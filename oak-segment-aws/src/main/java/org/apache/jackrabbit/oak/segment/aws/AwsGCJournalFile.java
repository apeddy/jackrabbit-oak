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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.jackrabbit.oak.segment.spi.persistence.GCJournalFile;
import org.apache.jackrabbit.oak.segment.spi.persistence.JournalFileReader;

public class AwsGCJournalFile implements GCJournalFile {

    private final AwsKinesisS3File file;

    public AwsGCJournalFile(AwsContext awsContext, String fileName) {
        this.file = new AwsKinesisS3File(awsContext, fileName, awsContext.getGCLogStreamName());
    }

    @Override
    public void writeLine(String line) throws IOException {
        file.openJournalWriter().writeLine(line);
    }

    @Override
    public List<String> readLines() throws IOException {
        JournalFileReader reader = file.openJournalReader();
        List<String> items = new ArrayList<>();

        String line;
        while ((line = reader.readLine()) != null) {
            items.add(line);
        }

        Collections.reverse(items);

        return items;
    }

    @Override
    public void truncate() throws IOException {
        file.openJournalWriter().truncate();
    }
}
