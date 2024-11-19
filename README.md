# Kafka Connect source connector for Bluesky

## Overview

Emits status updates from Bluesky to a Kafka topic.

## Config

|    | Notes | Default |
| -- | ----- | ------- |
| `bluesky.identity` | Username for the Bluesky API - e.g. `yourusername.bsky.social` |  |
| `bluesky.password` | App password for the Bluesky API - create one at https://bsky.app/settings/app-passwords  |   |
| `bluesky.searchterm` | The value to search for status updates with. This does not depend on a hashtag - it is a search for Bluesky posts that contain this word. |  `bluesky` |
| `bluesky.poll.ms` | Time interval (in milliseconds) to wait between submitting searches to the Bluesky API. Keep this high to avoid being rate limited. | `60000` (one minute) |
| `bluesky.topic` |  The name of the Kafka topic to deliver events to. | `bluesky` |

### Example output

With the JSON converter, events will look like:

```json
{
    "id": {
      "uri": "at://did:plc:gssjrx12z2xtqa7gl2rny3nm/app.bsky.feed.post/3lbcqs9nsnc2a",
      "cid": "bafybahfjnjjypu6ewy5iryvnm2nu3jwiajscbifi2dtgab7ogutkoi2caa"
    },
    "text": "User post in plain text here",
    "langs": [
      "en"
    ],
    "createdAt": 1732030120973,
    "author": {
      "handle": "username.bsky.social",
      "displayName": "User Name",
      "avatar": "https://cdn.bsky.app/img/avatar/plain/did:plc:gsx67r3nm/bafkoz1@jpeg"
    }
}
```

This is a subset of data available from the Bluesky API.

Refer to [Bluesky API docs](https://docs.bsky.app/docs/api/app-bsky-feed-search-posts) for an explanation of individual fields.
