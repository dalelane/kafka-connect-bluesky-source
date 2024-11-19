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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.co.dalelane.kafkaconnect.bluesky.api.Post;

public class BlueskySourceTask extends SourceTask {

    private static Logger log = LoggerFactory.getLogger(BlueskySourceTask.class);

    private BlueskyDataFetcher dataFetcher;
    private BlueskyRecordFactory recordFactory;


    @Override
    public void start(Map<String, String> properties) {
        log.info("Starting task {}", properties);

        AbstractConfig config = new AbstractConfig(BlueskyConfig.CONFIG_DEF, properties);
        log.debug("Config {}", config);

        recordFactory = new BlueskyRecordFactory(config);

        dataFetcher = new BlueskyDataFetcher(config, getOffset());
        dataFetcher.start();
    }


    @Override
    public void stop() {
        log.info("Stopping task");

        if (dataFetcher != null) {
            dataFetcher.stop();
        }

        dataFetcher = null;
        recordFactory = null;
    }


    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        List<Post> statuses = dataFetcher.getStatuses();
        return statuses.stream()
            .map(r -> recordFactory.createSourceRecord(r))
            .collect(Collectors.toList());
    }


    @Override
    public String version() {
        return BlueskySourceConnector.VERSION;
    }


    private String getOffset() {
        OffsetStorageReader offsetReader = getOffsetStorageReader();
        if (offsetReader != null) {
            return getTimestampFromPersistedOffset(offsetReader);
        }
        return null;
    }


    private OffsetStorageReader getOffsetStorageReader() {
        log.debug("getting offset storage reader");
        if (context == null) {
            log.debug("No context - assuming that this is the first time the Connector has run");
            return null;
        }
        else if (context.offsetStorageReader() == null) {
            log.debug("No offset reader - assuming that this is the first time the Connector has run");
            return null;
        }
        else {
            log.debug("Offset reader found");
            return context.offsetStorageReader();
        }
    }


    private static String getTimestampFromPersistedOffset(OffsetStorageReader persistedOffsetReader) {
        log.debug("getting timestamp from offset reader");
        Map<String, Object> persistedOffsetInfo = persistedOffsetReader.offset(BlueskyRecordFactory.createSourcePartition());
        Object offset = null;
        if (persistedOffsetInfo == null) {
            log.debug("no persisted offset available from reader");
        }
        else {
            offset = persistedOffsetInfo.get(BlueskyRecordFactory.OFFSET_FIELD);
        }
        if (offset != null) {
            log.debug("returning persisted offset {}", offset);
            return (String) offset;
        }
        else {
            log.debug("returning null offset");
            return null;
        }
    }
}
