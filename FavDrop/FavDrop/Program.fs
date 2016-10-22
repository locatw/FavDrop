open CoreTweet
open Dropbox.Api
open FSharpx.Control
open System.Configuration

module TwitterSource =
    let private withPhotos(status : Status) =
        status.Entities.Media
        |> Array.exists(fun media -> media.Type = "photo")

    let private queuePhoto (queue : BlockingQueueAgent<string>) (media : MediaEntity) =
        queue.Add(media.MediaUrl)
        System.Console.WriteLine("queued : {0}", media.MediaUrl)

    let private queuePhotos (queue : BlockingQueueAgent<string>) (status : Status) =
        status.Entities.Media
        |> Array.filter(fun media -> media.Type = "photo")
        |> Array.iter (queuePhoto queue)
    
    let run (queue : BlockingQueueAgent<string>) = async {
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
        |> Seq.iter (queuePhotos queue)
    }

module DropboxSink =
    let private saveFolderName = "photos"

    let private saveFolderPath = "/" + saveFolderName

    let private saveFolderExistsAsync (client : DropboxClient) = async {
        let existsInFolders (folders : Files.ListFolderResult)=
            folders.Entries
            |> List.ofSeq
            |> List.exists(fun entity -> entity.Name = saveFolderName)

        let rec checkRemainAsync (cursor : string) = async {
            let! folders = client.Files.ListFolderContinueAsync(cursor) |> Async.AwaitTask
            if existsInFolders folders then
                return true
            elif folders.HasMore then
                return! checkRemainAsync folders.Cursor
            else
                return false
        }

        let! foldersInRoot = client.Files.ListFolderAsync("") |> Async.AwaitTask
        if existsInFolders foldersInRoot then
            return true
        elif foldersInRoot.HasMore then
            return! checkRemainAsync foldersInRoot.Cursor
        else
            return false
    }

    let private createSaveFolderAsync (client : DropboxClient) = async {
        do! client.Files.CreateFolderAsync(saveFolderPath)
            |> Async.AwaitTask
            |> Async.Ignore
    }

    let private createSaveFolderIfNotExistsAsync (client : DropboxClient) = async {
        let! saveFolderExists = saveFolderExistsAsync client
        if not saveFolderExists then
            return! createSaveFolderAsync client
    }

    let run (queue : BlockingQueueAgent<string>) = async {
        let accessToken = ConfigurationManager.AppSettings.Item("DropboxAccessToken")
        use client = new DropboxClient(accessToken)

        do! createSaveFolderIfNotExistsAsync client

        System.Console.WriteLine("DropboxSink initialized")
        
        while true do
            let! photoUrl = queue.AsyncGet()
            System.Console.WriteLine("got photo url: {0}", photoUrl)

            let fileName = photoUrl.Split('/') |> Array.last
            let saveFilePath = saveFolderPath + "/" + fileName

            client.Files.SaveUrlAsync(saveFilePath, photoUrl)
            |> Async.AwaitTask
            |> Async.Ignore
            |> Async.Start
    }
        
[<EntryPoint>]
let main _ = 
    let queue = new BlockingQueueAgent<string>(100)

    [TwitterSource.run queue; DropboxSink.run queue]
    |> Async.Parallel
    |> Async.RunSynchronously
    |> ignore

    0
