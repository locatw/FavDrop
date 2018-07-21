open Dropbox.Api
open FavDrop
open FavDrop.ExponentialBackoff
open FavDrop.Domain
open Microsoft.Extensions.Configuration
open Microsoft.FSharp.Core.LanguagePrimitives
open System.Collections.Concurrent
open System.IO

[<EntryPoint>]
let main _ =
    let configurationBuilder =
        (new ConfigurationBuilder()).SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile("App.secret.json")
    let config = configurationBuilder.Build()

    let storageConnectionString = config.Item("StorageConnectionString")
    let queue = new ConcurrentQueue<FavoritedTweet>()
    let storage = new Storage.TableStorage(storageConnectionString, "FavDropAppLog")
    let retryConfig =
        { ExponentialBackoff.WaitTime = Int32WithMeasure(1000)
          ExponentialBackoff.MaxWaitTime = Int32WithMeasure(15 * 60 * 1000) }
    use logContext = Logging.makeContext storage retryAsync retryConfig
    let log = Logging.log logContext

    let accessToken = config.Item("DropboxAccessToken")
    use dropboxClient = new DropboxClient(accessToken)
    log Logging.Information "DropboxSink initialized"

    let dropboxFileClient = new DropboxSink.DropboxFileClient(dropboxClient)
    let retryConfig =
        { ExponentialBackoff.WaitTime = Int32WithMeasure(1000)
          ExponentialBackoff.MaxWaitTime = Int32WithMeasure(15 * 60 * 1000) }

    try
        try
            Logging.run logContext |> Async.Start

            [TwitterSource.run config log queue retryConfig; DropboxSink.run log dropboxFileClient queue retryConfig]
            |> Async.Parallel
            |> Async.RunSynchronously
            |> ignore
        with
        | :? System.Exception as e ->
            log Logging.Error (sprintf "Unhandled exception occurred. Exception: %s" (e.ToString()))
    finally
        Logging.cancel logContext

    0
