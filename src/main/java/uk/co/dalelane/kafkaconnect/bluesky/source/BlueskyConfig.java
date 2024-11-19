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

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;


/**
 * Config that the source connector expects - mostly connection
 *  details to Bluesky.
 */
public class BlueskyConfig {

    public static final String IDENTITY = "bluesky.identity";
    public static final String APP_PASSWORD = "bluesky.password";
    public static final String SEARCH_TERM = "bluesky.searchterm";
    public static final String POLL_INTERVAL_MS = "bluesky.poll.ms";
    public static final String TOPIC = "bluesky.topic";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
        .define(IDENTITY,
                Type.STRING,
                null,
                new ConfigDef.NonEmptyString(),
                Importance.HIGH,
                "Username / Identity for the Bluesky account")
        .define(APP_PASSWORD,
                Type.PASSWORD,
                null,
                Importance.HIGH,
                "App password for the Bluesky account")
        .define(SEARCH_TERM,
                Type.STRING,
                "bluesky",
                new ConfigDef.NonEmptyString(),
                Importance.HIGH,
                "Term to search for")
        .define(POLL_INTERVAL_MS,
                Type.INT,
                1000 * 60, // one minute
                ConfigDef.Range.atLeast(30_000),
                Importance.HIGH,
                "How long to wait (in milliseconds) between submitting searches to the Bluesky API")
        .define(TOPIC,
                Type.STRING,
                "bluesky",
                new ConfigDef.NonEmptyString(),
                Importance.HIGH,
                "Topic to deliver messages to");
}
