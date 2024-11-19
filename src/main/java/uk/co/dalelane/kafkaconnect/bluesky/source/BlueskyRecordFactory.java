/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package uk.co.dalelane.kafkaconnect.bluesky.source;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.source.SourceRecord;

import uk.co.dalelane.kafkaconnect.bluesky.api.Post;


public class BlueskyRecordFactory {

    // post property used as an offset in Connect records
    public static final String OFFSET_FIELD = "createdAt";

    // name of the topic to deliver messages to
    private String topic;


    public BlueskyRecordFactory(AbstractConfig config) {
        topic = config.getString(BlueskyConfig.TOPIC);
    }

    private static final Schema ACCOUNT_SCHEMA = SchemaBuilder.struct()
            .field("handle", Schema.STRING_SCHEMA)
            .field("displayName", Schema.OPTIONAL_STRING_SCHEMA)
            .field("avatar", Schema.OPTIONAL_STRING_SCHEMA)
        .build();

    private static final Schema ID_SCHEMA = SchemaBuilder.struct()
            .field("uri", Schema.STRING_SCHEMA)
            .field("cid", Schema.STRING_SCHEMA)
        .build();

    private static final Schema STATUS_SCHEMA = SchemaBuilder.struct()
        .name("status")
            .field("id", ID_SCHEMA)
            .field("text", Schema.STRING_SCHEMA)
            .field("langs", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
            .field("createdAt", Timestamp.builder().optional().build())
            .field("author", ACCOUNT_SCHEMA)
        .build();


    public SourceRecord createSourceRecord(Post data) {
        Instant timestampInstant = Instant.parse(data.createdAt);

        return new SourceRecord(createSourcePartition(),
                                createSourceOffset(data),
                                topic,
                                null,
                                null, null,
                                STATUS_SCHEMA, createStruct(data, timestampInstant),
                                timestampInstant.toEpochMilli());
    }

    public static Map<String, Object> createSourcePartition() {
        return null;
    }
    private Map<String, Object> createSourceOffset(Post data) {
        return Collections.singletonMap(OFFSET_FIELD, data.createdAt);
    }

    private Struct createStruct(Post data, Instant timestamp) {
        Struct accountStruct = new Struct(ACCOUNT_SCHEMA);
        accountStruct.put("handle", data.author.handle);
        if (!data.author.displayName.isEmpty()) {
            accountStruct.put("displayName", data.author.displayName);
        }
        if (!data.author.avatar.isEmpty()) {
            accountStruct.put("avatar", data.author.avatar);
        }

        Struct idStruct = new Struct(ID_SCHEMA);
        idStruct.put("uri", data.uri);
        idStruct.put("cid", data.cid);

        Struct statusStruct = new Struct(STATUS_SCHEMA);
        statusStruct.put("id", idStruct);
        statusStruct.put("text", data.text);
        statusStruct.put("langs", Arrays.asList(data.langs));
        statusStruct.put("createdAt", Date.from(timestamp));
        statusStruct.put("author", accountStruct);

        return statusStruct;
    }
}
