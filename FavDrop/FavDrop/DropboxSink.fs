module FavDrop.DropboxSink

open Dropbox.Api
open Dropbox.Api.Files
open ExponentialBackoff
open FavDrop.Domain
open FSharp.Data
open Microsoft.FSharp.Core.LanguagePrimitives
open System.Collections.Concurrent
open System.IO
open System.Text

type IDropboxFileClient =
    abstract member CreateFolderAsync : string -> Async<Dropbox.Api.Files.FolderMetadata>

    abstract member UploadAsync : CommitInfo -> Stream -> Async<Dropbox.Api.Files.FileMetadata>

type DropboxFileClient(client : DropboxClient) =
    let files = client.Files

    interface IDropboxFileClient with
        member __.CreateFolderAsync (path : string) =
            async {
                let! result = files.CreateFolderV2Async(path) |> Async.AwaitTask
                return result.Metadata
            }

        member __.UploadAsync (commitInfo : CommitInfo) (body : Stream) =
            files.UploadAsync(commitInfo, body) |> Async.AwaitTask

[<NoComparison>]
type DropboxFile =
| TweetInfoFile of string
| MediumFile of System.Uri

let private makeMediaFilePath (tweetFolderPath : string) (mediaUrl : string) =
    let fileName = mediaUrl .Split('/') |> Array.last
    tweetFolderPath + "/" + fileName

let private makeTweetInfoFilePath tweetFolderPath =
    tweetFolderPath + "/" + "TweetInfo.json"

let private makeMediaFiles tweet =
    let makePhotoFile photo =
        [new System.Uri(photo.Url)]

    let makeVideoFile video =
        [video.ThumnailUrl; video.VideoUrl]
        |> List.map (fun url -> new System.Uri(url))

    tweet.Media
    |> List.collect (function
                     | PhotoMedium photo -> makePhotoFile photo
                     | VideoMedium video -> makeVideoFile video
                     | AnimatedGifMedium gif -> makeVideoFile gif)
    |> List.map MediumFile

let private makeTweetInfo tweet =
    let makeTweetInfoMedia media =
        match media with
        | PhotoMedium x -> new TweetInfo.Media("photo", Some x.Url, None, None)
        | VideoMedium x -> new TweetInfo.Media("video", None, Some x.ThumnailUrl, Some x.VideoUrl)
        | AnimatedGifMedium x -> new TweetInfo.Media("animated_gif", None, Some x.ThumnailUrl, Some x.VideoUrl)

    let userId = match tweet.User.Id with
                 | Some x -> x
                 | None -> -1L
    let user = new TweetInfo.User(userId, tweet.User.Name, tweet.User.ScreenName)
    let media = tweet.Media
                |> List.map makeTweetInfoMedia
                |> List.toArray
    let json = TweetInfo.Tweet(
                "0.2",
                tweet.TweetId,
                user,
                tweet.CreatedAt.ToString(),
                tweet.FavoritedAt.ToString(),
                tweet.Text,
                media)
    TweetInfoFile(json.JsonValue.ToString())

let private makeDropboxFiles tweet =
    (makeTweetInfo tweet) :: (makeMediaFiles tweet)

let internal createTweetFolderWithRetryAsync (client : IDropboxFileClient) log retryConfig tweet =
    async {
        return! retryAsync retryConfig (fun () ->
            async {
                try
                    let tweetFolderPath = "/" + tweet.TweetId.ToString()

                    client.CreateFolderAsync tweetFolderPath |> ignore

                    return ExponentialBackoff.Success tweetFolderPath
                with
                | :? ApiException<CreateFolderError> as e ->
                    log Logging.Error (e.ToString())
                    return ExponentialBackoff.Retry
                | :? RateLimitException as e ->
                    log Logging.Error (e.ToString())
                    return ExponentialBackoff.Retry
            })
    }

let private downloadMediaAsync log retryConfig url =
    async {
        return! retryAsync retryConfig (fun () ->
            async {
                try
                    let! response = Http.AsyncRequest(url)
                    let data = match response.Body with
                               | Binary x -> x
                               | _ -> raise (new System.Exception("response body must be binary"))
                    return Success data
                with
                | :? RateLimitException as e ->
                    log Logging.Error (e.ToString())
                    return ExponentialBackoff.Retry
                | :? System.Net.WebException as e ->
                    log Logging.Warning (e.ToString())
                    return Retry
                | :? System.Exception as e ->
                    log Logging.Error (e.ToString())
                    return ExponentialBackoff.Retry
            })
    }

let private uploadAsync (client : IDropboxFileClient) log retryConfig filePath stream =
    async {
        do! retryAsync retryConfig (fun () ->
            async {
                try
                    let commitInfo = new CommitInfo(filePath, WriteMode.Overwrite.Instance)
                    do! client.UploadAsync commitInfo stream |> Async.Ignore
                    return ExponentialBackoff.Success ()
                with
                | :? ApiException<UploadError> as e ->
                    log Logging.Error (e.ToString())
                    return ExponentialBackoff.Retry
                | :? RateLimitException as e ->
                    log Logging.Error (e.ToString())
                    return ExponentialBackoff.Retry
                | :? System.Exception as e ->
                    log Logging.Error (e.ToString())
                    return ExponentialBackoff.Retry
            })
    }

let private saveTweetInfoAsync uploadAsync tweetFolderPath (tweetInfo : string) =
    async {
        let filePath = makeTweetInfoFilePath tweetFolderPath
        use memoryStream = new MemoryStream(Encoding.UTF8.GetBytes(tweetInfo))
        do! uploadAsync filePath memoryStream
    }

let private saveMediumAsync uploadAsync (downloadMediaAsync : string -> Async<byte[]>) tweetFolderPath (uri : System.Uri) =
    async {
        let uri = uri.AbsoluteUri
        let filePath = makeMediaFilePath tweetFolderPath uri
        let! data = downloadMediaAsync uri
        do! uploadAsync filePath (new MemoryStream(data))
    }

let private saveFavoritedTweetAsync uploadAsync downloadMediaAsync tweetFolderPath tweet =
    async {
        makeDropboxFiles tweet
        |> List.map (function
                     | TweetInfoFile file -> saveTweetInfoAsync uploadAsync tweetFolderPath file
                     | MediumFile uri -> saveMediumAsync uploadAsync downloadMediaAsync tweetFolderPath uri)
        |> Async.Parallel
        |> Async.RunSynchronously
        |> ignore
    }

let private storeTweet (client : IDropboxFileClient) (log : Logging.Log) retryConfig tweet =
    async {
        let downloadMediaAsync = downloadMediaAsync log retryConfig
        let uploadAsync = uploadAsync client log retryConfig

        let! tweetFolderPath = createTweetFolderWithRetryAsync client log retryConfig tweet
        do! saveFavoritedTweetAsync uploadAsync downloadMediaAsync tweetFolderPath tweet
    }

let run (log : Logging.Log) (client : IDropboxFileClient) (queue : ConcurrentQueue<FavoritedTweet>) retryConfig =
    async {
        let retryConfig =
            { ExponentialBackoff.WaitTime = Int32WithMeasure(1000)
              ExponentialBackoff.MaxWaitTime = Int32WithMeasure(15 * 60 * 1000) }
        
        while true do
            try
                let (isSucceeded, tweet) = queue.TryDequeue()
                if isSucceeded then
                    log Logging.Information (sprintf "got favorited tweet: %d" tweet.TweetId)

                    do! storeTweet client log retryConfig tweet
                else
                    do! Async.Sleep(1000)
            with
            | :? System.Exception as e ->
                log Logging.Error (e.ToString())
    }
