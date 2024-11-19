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
package uk.co.dalelane.kafkaconnect.bluesky.api;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.Charset;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BlueskyClient implements Closeable {

    private final Logger log = LoggerFactory.getLogger(BlueskyClient.class);

    private final HttpClient httpClient = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_2)
            .build();

    // cf. https://docs.bsky.app/docs/category/http-reference
    private static final String BLUESKY_API_BASE_URL = "https://bsky.social/xrpc/";
    private static final URI LOGIN_URI = URI.create(BLUESKY_API_BASE_URL + "com.atproto.server.createSession");
    private static final URI REFRESH_URI = URI.create(BLUESKY_API_BASE_URL + "com.atproto.server.refreshSession");

    // --------------------------------------------------------------
    //  CREDENTIALS
    // --------------------------------------------------------------

    // keeping credentials ready for login() call
    private final String username;
    private final String password;

    // bearer token to use for search calls
    private String accessJwt = null;
    // bearer token to use for refresh session calls
    private String refreshJwt = null;


    // --------------------------------------------------------------
    //  SESSION REFRESHING
    // --------------------------------------------------------------

    // accessjwt seems to expire after a few minutes, so refreshing
    //  every two minutes should be adequate
    private static final int SESSION_REFRESH_INTERVAL_MS = 1000 * 60 * 2;

    // as session refreshing is done in a background thread, we need
    //  to store it to be able to throw it on the next poll
    private BlueskyException refreshException = null;

    private Timer sessionRefreshTimer = null;
    private TimerTask sessionRefreshTask = new TimerTask() {
        @Override
        public void run() {
            refreshLogin();
        }
    };


    // --------------------------------------------------------------
    //  PAGINATION
    // --------------------------------------------------------------

    // timestamp (from the createdAt value) of the most recent post fetched
    //  should be a string containing a time stamp such as
    //   2024-09-30T19:40:02.943Z
    //  or null if no posts have ever been fetched
    private String lastPostTimestamp;



    /**
     *
     * @param lastPostTimestamp - can be null if this is the first time the client has run
     */
    public BlueskyClient(String username, String password, String lastPostTimestamp) {
        this.username = username;
        this.password = password;
        this.lastPostTimestamp = lastPostTimestamp;
    }


    public void login() throws BlueskyException {
        log.info("Logging into Bluesky as {}", username);
        try {
            JSONObject authData = new JSONObject()
                .put("identifier", username)
                .put("password", password);

            HttpRequest request = HttpRequest.newBuilder()
                .uri(LOGIN_URI)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(authData.toString()))
                .build();

            HttpResponse<String> response = httpClient.send(request,
                HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != 200) {
                log.error("Failed to log in - http {}", response.statusCode());
                throw new BlueskyException("Failed to log in to Bluesky - response code " + response.statusCode());
            }

            JSONObject jsonResponse = new JSONObject(response.body());
            accessJwt = jsonResponse.getString("accessJwt");
            refreshJwt = jsonResponse.getString("refreshJwt");

            log.debug("Starting background timer to refresh session jwt");
            sessionRefreshTimer = new Timer("bluesky-session-refresher");
            sessionRefreshTimer.schedule(sessionRefreshTask, SESSION_REFRESH_INTERVAL_MS, SESSION_REFRESH_INTERVAL_MS);
        }
        catch (IOException | InterruptedException exc) {
            log.error("Failure during login", exc);
            throw new BlueskyException("Failed to log in to Bluesky", exc);
        }
    }


    private void refreshLogin() {
        log.debug("Refreshing Bluesky login session");
        try {
            HttpRequest request = HttpRequest.newBuilder()
                .uri(REFRESH_URI)
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .header("Authorization", "Bearer " + refreshJwt)
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();

            HttpResponse<String> response = httpClient.send(request,
                HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != 200) {
                log.error("Failed to refresh session - http {}", response.statusCode());
                refreshException = new BlueskyException("Failed to refresh Bluesky session - response code " + response.statusCode());
            }

            log.debug("Storing new Bluesky access jwt");
            JSONObject jsonResponse = new JSONObject(response.body());
            accessJwt = jsonResponse.getString("accessJwt");
        }
        catch (IOException | InterruptedException exc) {
            log.error("Failure during session refresh", exc);
            refreshException = new BlueskyException("Failed to refresh Bluesky session", exc);
        }
    }


    private URI createSearchUrl(Map<String, String> parameters) {
        StringBuilder sb = new StringBuilder();
        sb.append(BLUESKY_API_BASE_URL);
        sb.append("app.bsky.feed.searchPosts");
        sb.append("?");
        for (String key : parameters.keySet()) {
            sb.append(key);
            sb.append("=");
            sb.append(parameters.get(key));
            sb.append("&");
        }
        return URI.create(sb.toString());
    }


    public List<Post> search(String searchterm, int limit) throws BlueskyException {
        if (refreshException != null) {
            throw refreshException;
        }

        List<Post> posts = new ArrayList<>();
        int numBefore = -1;
        while (posts.size() > numBefore) {
            numBefore = posts.size();
            searchPage(posts, searchterm, limit);
        }

        return posts;
    }

    private void searchPage(List<Post> searchResults, String searchterm, int limit) throws BlueskyException {
        log.info("Polling Bluesky for {} for posts using offset {}", searchterm, this.lastPostTimestamp);

        Map<String, String> queryParameters = new HashMap<>();
        // Search query string; syntax, phrase, boolean, and faceting is unspecified, but Lucene query syntax is recommended.
        queryParameters.put("q", URLEncoder.encode(searchterm, Charset.forName("UTF-8")));
        // maximum number of posts to fetch
        queryParameters.put("limit", Integer.toString(limit));
        // pagination
        if (lastPostTimestamp != null) {
            // search API is inclusive, so to avoid again fetching the last
            //  post again, we need to increment the last post timestamp
            //  by at least one millisecond
            String since = incrementTimestamp(lastPostTimestamp);
            if (since != null) {
                queryParameters.put("since", URLEncoder.encode(since, Charset.forName("UTF-8")));
            }
        }

        try {
            URI uri = createSearchUrl(queryParameters);
            log.debug("Submitting search {}", uri);

            HttpRequest request = HttpRequest.newBuilder()
                .uri(uri)
                .header("Accept", "application/json")
                .header("Authorization", "Bearer " + accessJwt)
                .GET()
                .build();

            HttpResponse<String> response = httpClient.send(request,
                HttpResponse.BodyHandlers.ofString());

            // parse the response
            //  the posts are returned in reverse order, so we
            //  iterate through the list in reverse to keep them
            //  in a chronological order
            JSONObject jsonResponse = new JSONObject(response.body());
            JSONArray posts = jsonResponse.getJSONArray("posts");
            for (int i = posts.length() - 1; i >= 0; i--) {
                JSONObject postData = posts.getJSONObject(i);
                Post post = parse(postData);
                if (post != null) {
                    searchResults.add(post);
                    lastPostTimestamp = post.createdAt;
                }
            }
        }
        catch (IOException | InterruptedException | JSONException exc) {
            log.error("Error while submitting search to Bluesky", exc);
        }
    }


    private Post parse(JSONObject postData) {
        try {
            JSONObject authorData = postData.getJSONObject("author");
            JSONObject recordData = postData.getJSONObject("record");

            User author = new User();
            author.handle = authorData.getString("handle");
            author.displayName = authorData.optString("displayName");
            author.avatar = authorData.optString("avatar");

            Post post = new Post();
            post.author = author;
            post.uri = postData.getString("uri");
            post.cid = postData.getString("cid");
            post.createdAt = recordData.getString("createdAt");
            if (recordData.has("langs")) {
                JSONArray langsJson = recordData.getJSONArray("langs");
                post.langs = new String[langsJson.length()];
                for (int i=0; i < langsJson.length(); i++) {
                    post.langs[i] = langsJson.getString(i);
                }
            }
            else {
                post.langs = new String[0];
            }
            post.text = recordData.getString("text");

            return post;
        }
        catch (JSONException exc) {
            log.error("Failed to parse Bluesky post - skipping post : {}", postData.toString());
            log.error("json parse exception", exc);
            return null;
        }
    }


    private String incrementTimestamp(String input) {
        if (input != null) {
            try {
                Instant timestampInstant = Instant.parse(input);
                long incrementedVal = timestampInstant.toEpochMilli() + 1;
                return Instant.ofEpochMilli(incrementedVal).toString();
            }
            catch (DateTimeParseException exc) {
                log.error("Failed to parse createdAt timestamp {} from Bluesky post", input);
            }
        }
        return null;
    }



    public void logout() {
        if (sessionRefreshTimer != null) {
            sessionRefreshTimer.cancel();
        }
        accessJwt = null;
        refreshJwt = null;
    }


    @Override
    public void close() throws IOException {
        logout();
    }
}
