open CoreTweet
open Dropbox.Api
open FSharp.Data
open FSharpx.Control
open System
open System.Configuration
open System.IO
open System.Text

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

module TwitterSource =
    let private convertPhotoMedium (media : CoreTweet.MediaEntity) =
        { PhotoMedium.Url = media.MediaUrlHttps }

    let private convertVideoMedia (media : CoreTweet.MediaEntity) =
        let largeVideo = media.VideoInfo.Variants
                         |> Array.filter (fun variant -> let x = variant.Bitrate in x.HasValue) // deal with warning
                         |> Array.maxBy (fun variant -> let x = variant.Bitrate in x.Value) // deal with warning

        // Use MediaUrl instead of MediaUrlHttps because currupted thumnail image is saved by Dropbox when use MediaUrlHttps.
        { VideoMedium.ThumnailUrl = media.MediaUrl
          VideoUrl = largeVideo.Url }

    let private convertMedia (media : CoreTweet.MediaEntity) =
        match media.Type with
        | "photo" -> PhotoMedium (convertPhotoMedium media)
        | "video" -> VideoMedium (convertVideoMedia media)
        | _ -> raise (System.NotSupportedException("unsupported media type"))

    let private convertTweet (tweet : Status) = 
        let media = tweet.ExtendedEntities.Media
                    |> Array.map convertMedia
                    |> Array.toList
        let tweetUserId = tweet.User.Id
        let user = { Id = if tweetUserId.HasValue then
                            Some tweetUserId.Value
                          else
                            None
                     Name = tweet.User.Name
                     ScreenName = tweet.User.ScreenName }
        { TweetId = tweet.Id
          User = user
          CreatedAt = tweet.CreatedAt
          FavoritedAt = DateTimeOffset.Now
          Text = match tweet.FullText with
                 | null -> tweet.Text
                 | _ -> tweet.Text
          Media = media }

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

    let private savePhotoMediaAsync (client : DropboxClient) (makeMediaFilePath : string -> string) (photo : PhotoMedium) = async {
        let filePath = makeMediaFilePath photo.Url
        return! client.Files.SaveUrlAsync(filePath, photo.Url)
                |> Async.AwaitTask
                |> Async.Ignore
    }

    let private saveVideoMediaAsync (client : DropboxClient) (makeMediaFilePath : string -> string) (video: VideoMedium) = async {
        return! [video.ThumnailUrl;  video.VideoUrl]
                |> List.map (fun url -> (url, makeMediaFilePath url))
                |> List.map (fun (url, filePath) -> client.Files.SaveUrlAsync(filePath, url))
                |> List.map Async.AwaitTask
                |> Async.Parallel
                |> Async.Ignore
    }

    let private saveMediaAsync (client : DropboxClient) (makeMediaFilePath : string -> string) (medium : Medium) = async {
        return! match medium with
                | PhotoMedium x -> savePhotoMediaAsync client makeMediaFilePath x
                | VideoMedium x -> saveVideoMediaAsync client makeMediaFilePath x
    }

    let private saveTweetInfoAsync (client : DropboxClient) tweetFolderPath (tweet : FavoritedTweet) = async {
        let userId = match tweet.User.Id with
                     | Some x -> x
                     | None -> -1L
        let user = new TweetInfo.User(userId, tweet.User.Name, tweet.User.ScreenName)
        let media = tweet.Media
                    |> List.map (fun media ->
                                    match media with
                                    | PhotoMedium x -> new TweetInfo.Media("photo", Some x.Url, None, None)
                                    | VideoMedium x -> new TweetInfo.Media("video", None, Some x.ThumnailUrl, Some x.VideoUrl))
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
