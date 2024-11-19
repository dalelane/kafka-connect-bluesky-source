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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.co.dalelane.kafkaconnect.bluesky.api.BlueskyClient;
import uk.co.dalelane.kafkaconnect.bluesky.api.BlueskyException;
import uk.co.dalelane.kafkaconnect.bluesky.api.Post;



public class BlueskyDataFetcher {

    private static Logger log = LoggerFactory.getLogger(BlueskyDataFetcher.class);

    // the search term used in Bluesky searches - not treated as a hashtag, just
    //  using freetext search
    private final String searchterm;

    // posts retrieved from Bluesky that haven't yet been collected by
    //  the Kafka Connect task
    private final List<Post> fetchedRecords = Collections.synchronizedList(new ArrayList<Post>());

    // flag if the fetcher should currently be running
    private boolean isRunning;

    // how long to wait before polling the Bluesky API again
    private final int pollInterval;

    // number of records to fetch from Bluesky in each poll - hard-coded
    //  to the maximum that the Bluesky API allows
    // cf. https://docs.bsky.app/docs/api/app-bsky-feed-search-posts
    private static final int POLL_BATCH_SIZE = 100;

    // connection to Bluesky
    private BlueskyClient blueskyClient = null;
    private BlueskyException connectionError = null;


    private Timer pollTimer;
    private TimerTask pollTask = new TimerTask() {
        @Override
        public void run() {
            if (blueskyClient != null) {
                try {
                    List<Post> posts = blueskyClient.search(searchterm, POLL_BATCH_SIZE);
                    if (posts.size() > 0) {
                        synchronized (fetchedRecords) {
                            for (Post post : posts) {
                                fetchedRecords.add(post);
                            }
                        }
                    }
                }
                catch (BlueskyException e) {
                    log.debug("Failed to fetch records from Bluesky", e);
                    connectionError = e;
                }
            }
        }
    };



    public BlueskyDataFetcher(AbstractConfig config, String offset) {
        log.info("Creating a Bluesky data fetcher");

        // initialise variables
        isRunning = false;
        searchterm = config.getString(BlueskyConfig.SEARCH_TERM);
        pollInterval = config.getInt(BlueskyConfig.POLL_INTERVAL_MS);

        // prepare Bluesky client (credentials are not validated until login())
        blueskyClient = new BlueskyClient(
            config.getString(BlueskyConfig.IDENTITY),
            config.getPassword(BlueskyConfig.APP_PASSWORD).value(),
            offset
        );
    }


    public List<Post> getStatuses() throws ConnectException {
        if (connectionError != null) {
            throw new ConnectException(connectionError);
        }

        if (fetchedRecords.size() > 0) {
            synchronized (fetchedRecords) {
                List<Post> copy = new ArrayList<>(fetchedRecords);
                fetchedRecords.clear();
                return copy;
            }
        }
        else {
            return Collections.emptyList();
        }
    }


    public synchronized void start() throws ConnectException {
        log.debug("Starting Bluesky fetcher");

        try {
            blueskyClient.login();
        }
        catch (BlueskyException e) {
            log.error("Login failure during startup");
            throw new ConnectException(e);
        }

        if (isRunning == false) {
            pollTimer = new Timer("bluesky-posts-poller");
            pollTimer.schedule(pollTask, 5000, pollInterval);

            isRunning = true;
        }
    }


    public void stop() {
        log.debug("Stopping Bluesky fetcher");

        if (isRunning) {
            pollTimer.cancel();
            isRunning = false;
        }

        if (blueskyClient != null) {
            blueskyClient.logout();

            blueskyClient = null;
        }

        connectionError = null;
    }
}
