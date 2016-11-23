﻿module TwitterSource

open CoreTweet
open FavDrop.Domain
open FSharp.Control
open FSharpx
open FSharpx.Option
open Microsoft.FSharp.Core.LanguagePrimitives
open System
open System.Configuration

let private convertPhotoMedium (media : CoreTweet.MediaEntity) =
    { PhotoMedium.Url = media.MediaUrlHttps }

let private convertVideoMedia (media : CoreTweet.MediaEntity) =
    let largeVideo = media.VideoInfo.Variants
                     |> Array.filter (fun variant -> let x = variant.Bitrate in x.HasValue) // deal with warning
                     |> Array.maxBy (fun variant -> let x = variant.Bitrate in x.Value) // deal with warning

    // Use MediaUrl instead of MediaUrlHttps because currupted thumnail image is saved by Dropbox when use MediaUrlHttps.
    { ThumnailUrl = media.MediaUrl
      VideoUrl = largeVideo.Url }

let private convertMedia (media : CoreTweet.MediaEntity) =
    match media.Type with
    | "photo" -> PhotoMedium (convertPhotoMedium media)
    | "video" -> VideoMedium (convertVideoMedia media)
    | "animated_gif" -> AnimatedGifMedium (convertVideoMedia media)
    | _ -> raise (System.NotSupportedException("unsupported media type"))

let private convertTweet (tweet : Status) = 
    let media = tweet.ExtendedEntities.Media
                |> Array.map convertMedia
                |> Array.toList
    let tweetUserId = tweet.User.Id
    let userId =
        if tweetUserId.HasValue then
          Some tweetUserId.Value
        else
          None
    let user = { Id = userId; Name = tweet.User.Name; ScreenName = tweet.User.ScreenName }
    let text = match tweet.FullText with
               | null | "" -> tweet.Text
               | _ -> tweet.FullText
    { TweetId = tweet.Id
      User = user
      CreatedAt = tweet.CreatedAt
      FavoritedAt = DateTimeOffset.Now
      Text = text
      Media = media }

let private withMedia (tweet : Status) =
    let isMediaInEntities (entities : CoreTweet.Entities) =
        let media =
            maybe {
                let! entities = entities |> FSharpOption.ToFSharpOption
                return! entities.Media |> FSharpOption.ToFSharpOption
            }
        match media with
        | Some x -> 0 < x.Length
        | None -> false
    [tweet.Entities; tweet.ExtendedEntities]
    |> List.forall isMediaInEntities

let private queueTweet (log : Logging.Severity -> string -> unit) (queue : BlockingQueueAgent<FavoritedTweet>) favoritedTweet =
    queue.Add(favoritedTweet)
    log Logging.Information (sprintf "tweet queued : %d" favoritedTweet.TweetId)

let private processTweet (log : Logging.Severity -> string -> unit) (token : CoreTweet.Tokens) (queue : BlockingQueueAgent<FavoritedTweet>) =
    token.Streaming.User()
    |> Seq.filter(fun msg -> msg :? Streaming.EventMessage)
    |> Seq.map(fun msg -> msg :?> Streaming.EventMessage)
    |> Seq.filter(fun msg -> msg.Event = Streaming.EventCode.Favorite)
    |> Seq.map(fun msg -> msg.TargetStatus)
    |> Seq.filter withMedia
    |> Seq.map convertTweet
    |> Seq.iter (queueTweet log queue)

let run (log : Logging.Severity -> string -> unit)
        (queue : BlockingQueueAgent<FavoritedTweet>)
        (retryAsync : ExponentialBackoff.RetryConfig -> (unit -> ExponentialBackoff.RetryActionResult) -> Async<unit>) = async {
    let consumerKey = ConfigurationManager.AppSettings.Item("TwitterConsumerKey")
    let consumerSecret = ConfigurationManager.AppSettings.Item("TwitterConsumerSecret")
    let accessToken = ConfigurationManager.AppSettings.Item("TwitterAccessToken")
    let accessTokenSecret = ConfigurationManager.AppSettings.Item("TwitterAccessSecret")

    let token = Tokens.Create(consumerKey, consumerSecret, accessToken, accessTokenSecret)

    log Logging.Information "TwitterSource initialized"

    let retryConfig =
        { ExponentialBackoff.WaitTime = Int32WithMeasure(1000)
          ExponentialBackoff.MaxWaitTime = Int32WithMeasure(15 * 60 * 1000) }

    let f () =
        let startTime = DateTime.UtcNow
        try
            processTweet log token queue
            ExponentialBackoff.NoRetry
        with
        | :? System.Net.WebException ->
            let curTime = DateTime.UtcNow
            let diff = curTime.Subtract(startTime)
            match (int diff.TotalSeconds) with
            | x when x <= (int retryConfig.MaxWaitTime) + 5000 -> ExponentialBackoff.Retry
            | _ -> ExponentialBackoff.NoRetry

    while true do
        do! (retryAsync retryConfig) f
}
