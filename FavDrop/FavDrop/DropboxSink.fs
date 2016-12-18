module FavDrop.DropboxSink

open Dropbox.Api
open Dropbox.Api.Files
open FavDrop.Domain
open FSharp.Data
open Microsoft.FSharp.Core.LanguagePrimitives
open System.Collections.Concurrent
open System.Configuration
open System.IO
open System.Text

type IDropboxFileClient =
    abstract member CreateFolderAsync : string -> Async<Dropbox.Api.Files.FolderMetadata>

    abstract member UploadAsync : CommitInfo -> Stream -> Async<Dropbox.Api.Files.FileMetadata>

type DropboxFileClient(client : DropboxClient) =
    let files = client.Files

    interface IDropboxFileClient with
        member __.CreateFolderAsync (path : string) =
            files.CreateFolderAsync(path) |> Async.AwaitTask

        member __.UploadAsync (commitInfo : CommitInfo) (body : Stream) =
            files.UploadAsync(commitInfo, body) |> Async.AwaitTask

[<NoComparison>]
type DropboxFile =
| TweetInfoFile of string
| MediumFile of System.Uri

let private saveFolderPath = ""

let private createTweetFolderAsync (client : IDropboxFileClient) tweet =
    async {
        let tweetFolderPath = saveFolderPath + "/" + tweet.TweetId.ToString()

        client.CreateFolderAsync tweetFolderPath |> ignore

        return tweetFolderPath
    }

let private makeMediaFilePath (tweetFolderPath : string) (mediaUrl : string) =
    let fileName = mediaUrl .Split('/') |> Array.last
    tweetFolderPath + "/" + fileName

let private makeTweetInfoFilePath tweetFolderPath =
    tweetFolderPath + "/" + "TweetInfo.json"

let private saveMediumAsync (client : IDropboxFileClient) (makeMediaFilePath : string -> string) (uri : System.Uri) =
    async {
        let url = uri.AbsoluteUri
        let! data = Http.AsyncRequestStream(url)
        let filePath = makeMediaFilePath url
        let commitInfo = new CommitInfo(filePath, WriteMode.Overwrite.Instance)
        do! client.UploadAsync commitInfo data.ResponseStream |> Async.Ignore
    }

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

let private saveTweetInfoAsync (client : IDropboxFileClient) (makeTweetInfoFilePath : unit -> string) (tweetInfo: string) =
    async {
        let filePath = makeTweetInfoFilePath()
        let commitInfo = new CommitInfo(filePath, WriteMode.Overwrite.Instance)
        use memoryStream = new MemoryStream(Encoding.UTF8.GetBytes(tweetInfo))
        return! client.UploadAsync commitInfo memoryStream |> Async.Ignore
    }

let private saveWithRetryAsync retryAsync log saveAsync =
    async {
        do! retryAsync (fun () ->
            async {
                try
                    do! saveAsync()
                    return ExponentialBackoff.NoRetry
                with
                | :? ApiException<CreateFolderError> as e ->
                    log Logging.Error (e.ToString())
                    return ExponentialBackoff.Retry
                | :? ApiException<UploadError> as e ->
                    log Logging.Error (e.ToString())
                    return ExponentialBackoff.Retry
                | :? System.Exception as e ->
                    log Logging.Error (e.ToString())
                    return ExponentialBackoff.Retry
            })
    }

let private saveTweetInfoWithRetryAsync client retryAsync log tweetFolderPath tweetInfo =
    saveWithRetryAsync retryAsync log (fun () -> saveTweetInfoAsync client tweetFolderPath tweetInfo)

let private saveMediumWithRetryAsync client retryAsync log (makeMediaFilePath : string -> string) (uri : System.Uri) =
    saveWithRetryAsync retryAsync log (fun () -> saveMediumAsync client makeMediaFilePath uri)

let private saveFavoritedTweetAsync saveTweetInfoAsync saveMediumAsync tweet =
    async {
        makeDropboxFiles tweet
        |> List.map (function
                     | TweetInfoFile file -> saveTweetInfoAsync file
                     | MediumFile file -> saveMediumAsync file)
        |> Async.Parallel
        |> Async.RunSynchronously
        |> ignore
    }

let private storeTweet (client : IDropboxFileClient) (log : Logging.Log) (retryAsync : (unit -> Async<ExponentialBackoff.RetryActionResult>) -> Async<unit>) (queue : ConcurrentQueue<FavoritedTweet>) =
    async {
        try
            let (isSucceeded, tweet) = queue.TryDequeue()
            if isSucceeded then
                log Logging.Information (sprintf "got favorited tweet: %d" tweet.TweetId)

                let! tweetFolderPath = createTweetFolderAsync client tweet
                let saveTweetInfoAsync = saveTweetInfoWithRetryAsync client retryAsync log (fun () -> makeTweetInfoFilePath tweetFolderPath)
                let saveMediumAsync = saveMediumWithRetryAsync client retryAsync log (makeMediaFilePath tweetFolderPath)

                do! saveFavoritedTweetAsync saveTweetInfoAsync saveMediumAsync tweet
            else
                do! Async.Sleep(1000)
        with
        | :? System.Exception as e ->
            log Logging.Error (e.ToString())
    }

let run (log : Logging.Log) (client : IDropboxFileClient) (queue : ConcurrentQueue<FavoritedTweet>) (retryAsync : ExponentialBackoff.RetryAsync) =
    async {
        let retryConfig =
            { ExponentialBackoff.WaitTime = Int32WithMeasure(1000)
              ExponentialBackoff.MaxWaitTime = Int32WithMeasure(15 * 60 * 1000) }
        
        while true do
            do! storeTweet client log (retryAsync retryConfig) queue
    }
