open FavDrop
open FavDrop.ExponentialBackoff
open FavDrop.Domain
open FSharp.Control
open Microsoft.Azure
open Microsoft.FSharp.Core.LanguagePrimitives
open System
open System.Configuration

[<EntryPoint>]
let main _ = 
    let storageConnectionString =
        CloudConfigurationManager.GetSetting("StorageConnectionString")
    let queue = new BlockingQueueAgent<FavoritedTweet>(100)
    let storage = new Storage.TableStorage(storageConnectionString, "FavDropAppLog")
    let retryConfig =
        { ExponentialBackoff.WaitTime = Int32WithMeasure(1000)
          ExponentialBackoff.MaxWaitTime = Int32WithMeasure(15 * 60 * 1000) }
    use logContext = Logging.makeContext storage retryAsync retryConfig
    let log = Logging.log logContext

    Logging.run logContext |> Async.Start

    [TwitterSource.run log queue retryAsync; DropboxSink.run log queue]
    |> Async.Parallel
    |> Async.RunSynchronously
    |> ignore

    Logging.cancel logContext
    
    0
