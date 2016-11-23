﻿module FavDrop.DropboxSink

open Dropbox.Api
open FavDrop.Domain
open FSharp.Control
open FSharp.Data
open System.Configuration
open System.IO
open System.Text

let private saveFolderPath = ""

let private createTweetFolderAsync (client : DropboxClient) tweet = async {
    let tweetFolderPath = saveFolderPath + "/" + tweet.TweetId.ToString()

    client.Files.CreateFolderAsync(tweetFolderPath)
    |> Async.AwaitTask
    |> ignore

    return tweetFolderPath
}

let private makeMediaFilePath (tweetFolderPath : string) (mediaUrl : string) =
    let fileName = mediaUrl .Split('/') |> Array.last
    tweetFolderPath + "/" + fileName

let private makeTweetInfoFilePath tweetFolderPath =
    tweetFolderPath + "/" + "TweetInfo.json"

let private saveMediumAsync (client : DropboxClient) (makeMediaFilePath : string -> string) (url : string) = async {
    let! data = Http.AsyncRequestStream(url)
    let filePath = makeMediaFilePath url
    do! client.Files.UploadAsync(filePath, Files.WriteMode.Overwrite.Instance, body = data.ResponseStream)
            |> Async.AwaitTask
            |> Async.Ignore
}

let private savePhotoMediaAsync (client : DropboxClient) (makeMediaFilePath : string -> string) (photo : PhotoMedium) = async {
    do! saveMediumAsync client makeMediaFilePath photo.Url
}

let private saveVideoMediaAsync (client : DropboxClient) (makeMediaFilePath : string -> string) (video: VideoMedium) = async {
    return! [video.ThumnailUrl;  video.VideoUrl]
            |> List.map (saveMediumAsync client makeMediaFilePath)
            |> Async.Parallel
            |> Async.Ignore
}

let private saveMediaAsync (client : DropboxClient) (makeMediaFilePath : string -> string) (medium : Medium) = async {
    return! match medium with
            | PhotoMedium x -> savePhotoMediaAsync client makeMediaFilePath x
            | VideoMedium x -> saveVideoMediaAsync client makeMediaFilePath x
            | AnimatedGifMedium x -> saveVideoMediaAsync client makeMediaFilePath x
}

let private saveTweetInfoAsync (client : DropboxClient) tweetFolderPath (tweet : FavoritedTweet) = async {
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
    let jsonText = json.JsonValue.ToString()

    use memoryStream = new MemoryStream(Encoding.UTF8.GetBytes(jsonText))
    let filePath = makeTweetInfoFilePath tweetFolderPath
    return! client.Files.UploadAsync(filePath, Files.WriteMode.Overwrite.Instance, body = memoryStream)
            |> Async.AwaitTask
            |> Async.Ignore
}

let private saveFavoritedTweetAsync (client : DropboxClient) tweetFolderPath tweet = async {
    let saveMedia = tweet.Media
                    |> List.map (saveMediaAsync client (makeMediaFilePath tweetFolderPath))
    let saveTweetInfo = saveTweetInfoAsync client tweetFolderPath tweet

    saveTweetInfo :: saveMedia
    |> Async.Parallel
    |> Async.RunSynchronously
    |> ignore
}

let run (log : Logging.Log) (queue : BlockingQueueAgent<FavoritedTweet>) = async {
    let accessToken = ConfigurationManager.AppSettings.Item("DropboxAccessToken")
    use client = new DropboxClient(accessToken)

    log Logging.Information "DropboxSink initialized"
    
    while true do
        let! tweet = queue.AsyncGet()

        log Logging.Information (sprintf "got favorited tweet: %d" tweet.TweetId)

        let! tweetFolderPath = createTweetFolderAsync client tweet
        do! saveFavoritedTweetAsync client tweetFolderPath tweet
}