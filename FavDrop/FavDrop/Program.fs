open CoreTweet
open Dropbox.Api
open FSharpx.Control
open System.Configuration

type FavoritedTweet = {
    TweetId : int64
    MediaUrls : string list
}

module TwitterSource =
    let private convertTweet (tweet : Status) = 
        let mediaUrls = tweet.Entities.Media
                        |> Array.map (fun media -> media.MediaUrl)
                        |> Array.toList
        { TweetId = tweet.Id; MediaUrls = mediaUrls }

    let private withPhotos(status : Status) =
        status.Entities.Media
        |> Array.exists(fun media -> media.Type = "photo")

    let private queueTweet (queue : BlockingQueueAgent<FavoritedTweet>) favoritedTweet =
        queue.Add(favoritedTweet)
        System.Console.WriteLine("tweet queued : {0}", favoritedTweet.TweetId)
    
    let run (queue : BlockingQueueAgent<FavoritedTweet>) = async {
        let consumerKey = ConfigurationManager.AppSettings.Item("TwitterConsumerKey")
        let consumerSecret = ConfigurationManager.AppSettings.Item("TwitterConsumerSecret")
        let accessToken = ConfigurationManager.AppSettings.Item("TwitterAccessToken")
        let accessTokenSecret = ConfigurationManager.AppSettings.Item("TwitterAccessSecret")
        let token = Tokens.Create(consumerKey, consumerSecret, accessToken, accessTokenSecret)

        System.Console.WriteLine("TwitterSource initialized")

        token.Streaming.User()
        |> Seq.filter(fun msg -> msg :? Streaming.EventMessage)
        |> Seq.map(fun msg -> msg :?> Streaming.EventMessage)
        |> Seq.filter(fun msg -> msg.Event = Streaming.EventCode.Favorite)
        |> Seq.map(fun msg -> msg.TargetStatus)
        |> Seq.filter withPhotos
        |> Seq.map convertTweet
        |> Seq.iter (queueTweet queue)
    }

module DropboxSink =
    let private saveFolderPath = "/"

    let private createTweetFolderAsync (client : DropboxClient) tweet = async {
        let tweetFolderPath = saveFolderPath + "/" + tweet.TweetId.ToString()

        client.Files.CreateFolderAsync(tweetFolderPath)
        |> Async.AwaitTask
        |> ignore

        return tweetFolderPath
    }

    let private saveMediaAsync (client : DropboxClient) tweetFolderPath (mediaUrl : string) = async {
        let fileName = mediaUrl.Split('/') |> Array.last
        let filePath = tweetFolderPath + "/" + fileName
        return! client.Files.SaveUrlAsync(filePath, mediaUrl)
                |> Async.AwaitTask
    }

    let private saveFavoritedTweetAsync (client : DropboxClient) tweetFolderPath tweet = async {
        tweet.MediaUrls
        |> List.map (saveMediaAsync client tweetFolderPath)
        |> Async.Parallel
        |> Async.RunSynchronously
        |> ignore
    }

    let run (queue : BlockingQueueAgent<FavoritedTweet>) = async {
        let accessToken = ConfigurationManager.AppSettings.Item("DropboxAccessToken")
        use client = new DropboxClient(accessToken)

        System.Console.WriteLine("DropboxSink initialized")
        
        while true do
            let! tweet = queue.AsyncGet()

            System.Console.WriteLine("got favorited tweet: {0}", tweet.TweetId)

            let! tweetFolderPath = createTweetFolderAsync client tweet
            do! saveFavoritedTweetAsync client tweetFolderPath tweet
    }
        
[<EntryPoint>]
let main _ = 
    let queue = new BlockingQueueAgent<FavoritedTweet>(100)

    [TwitterSource.run queue; DropboxSink.run queue]
    |> Async.Parallel
    |> Async.RunSynchronously
    |> ignore

    0
