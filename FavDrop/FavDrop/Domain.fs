module FavDrop.Domain

open FSharp.Data
open System

type PhotoMedium = {
    Url : string
}

type VideoMedium = {
    ThumnailUrl : string
    VideoUrl : string
}

type Medium =
| PhotoMedium of PhotoMedium
| VideoMedium of VideoMedium
| AnimatedGifMedium of VideoMedium

type TwitterUser = {
    Id : int64 option
    Name : string
    ScreenName : string
}

type FavoritedTweet = {
    TweetId : int64
    User : TwitterUser
    CreatedAt : DateTimeOffset
    FavoritedAt : DateTimeOffset
    Text : string
    Media : Medium list
}

// I want to use DateTimeOffset to create_at and favorited_at,
// but JsonProvider doesn't support DateTimeOffset.
// Insted, use string with timezone.
[<Literal>]
let tweetInfoSample = """
{
  "format_version": "[version number]",
  "id": 1000000000000,
  "user": {
    "id": 1000000000000,
    "name": "user_name",
    "screen_name": "screen_name"
  },
  "created_at": "string represents the date",
  "favorited_at": "string represents the date",
  "text": "text",
  "media": [
    {
      "type": "photo",
      "url": "https://www.sample.com/"
    },
    {
      "type": "video",
      "thumnail_url": "https://www.sample.com/",
      "video_url": "https://www.sample.com/"
    }
  ]
}
"""

type TweetInfo = JsonProvider<tweetInfoSample, RootName = "tweet">
