module FavDrop.TwitterSource

open CoreTweet
open FavDrop.Domain
open Microsoft.Extensions.Configuration
open System
open System.Collections.Concurrent
open System.Threading

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
        match entities.Media with
        | [||] -> false
        | null -> false
        | _ -> true
    [tweet.Entities; tweet.ExtendedEntities]
    |> List.forall isMediaInEntities

let private queueTweet (log : Logging.Log) (queue : ConcurrentQueue<FavoritedTweet>) favoritedTweet =
    queue.Enqueue(favoritedTweet)
    log Logging.Information (sprintf "tweet queued : %d" favoritedTweet.TweetId)

let private processTweet (log : Logging.Log) (queue : ConcurrentQueue<FavoritedTweet>) (message : Streaming.StreamingMessage) =
    match message with
    | :? Streaming.EventMessage as msg when msg.Event = Streaming.EventCode.Favorite && withMedia msg.TargetStatus ->
        queueTweet log queue (convertTweet msg.TargetStatus)
    | :? Streaming.DisconnectMessage
    | :? Streaming.WarningMessage
    | :? Streaming.LimitMessage
    | :? Streaming.RawJsonMessage ->
        log Logging.Warning (sprintf "Error message received from twitter: %s" (message.ToString()))
    | _ ->
        ()

let private createTweetObserver next completed error =
    {
        new IObserver<Streaming.StreamingMessage> with
            member __.OnCompleted () = completed ()
            member __.OnError(e : Exception) = error e
            member __.OnNext(message : Streaming.StreamingMessage) = next message
    }

let run (config : IConfigurationRoot) (log : Logging.Log) (queue : ConcurrentQueue<FavoritedTweet>) (retryConfig : ExponentialBackoff.RetryConfig) =
    async {
        let consumerKey = config.Item("TwitterConsumerKey")
        let consumerSecret = config.Item("TwitterConsumerSecret")
        let accessToken = config.Item("TwitterAccessToken")
        let accessTokenSecret = config.Item("TwitterAccessSecret")

        let token = Tokens.Create(consumerKey, consumerSecret, accessToken, accessTokenSecret)

        log Logging.Information "TwitterSource initialized"

        let f () =
            async {
                let startTime = DateTime.UtcNow
                use waitEvent = new ManualResetEvent(false)
                let mutable result = ExponentialBackoff.Success()

                let next = processTweet log queue
                let completed () =
                    result <- ExponentialBackoff.Success()
                    if waitEvent.Set() then
                        ()
                    else
                        raise (ApplicationException("Cannot set state of the event to signaled."))
                let error (e : Exception) =
                    match e with
                    | :? TwitterException as e ->
                        let curTime = DateTime.UtcNow
                        let diff = curTime.Subtract(startTime)
                        match (int diff.TotalSeconds) with
                        | x when x <= (int retryConfig.MaxWaitTime) + 5000 ->
                            log Logging.Error (sprintf "TwitterException occurred in TwitterSource. Exception: %s" (e.ToString()))
                            result <- ExponentialBackoff.Retry
                        | _ ->
                            let messageBuilder = new System.Text.StringBuilder()
                            messageBuilder.Append("TwitterException occurred in TwitterSource.") |> ignore
                            messageBuilder.Append(" This is the first error after normal process started and may be disconnected from twitter.") |> ignore
                            messageBuilder.AppendFormat(" Exception: %s", e.ToString()) |> ignore
                            log Logging.Error (messageBuilder.ToString())
                            result <- ExponentialBackoff.Success()
                    | :? Exception as e ->
                        log Logging.Error (sprintf "Exception occurred in TwitterSource. Exception: %s" (e.ToString()))
                        result <- ExponentialBackoff.Success()
                    waitEvent.Set() |> ignore
                let observer = createTweetObserver next completed error

                use _ = token.Streaming.UserAsObservable().Subscribe(observer)

                waitEvent.WaitOne(Timeout.Infinite) |> ignore
                return result
            }

        while true do
            log Logging.Debug "one process start"
            do! ExponentialBackoff.retryAsync retryConfig f
            log Logging.Debug "one process end"
    }
