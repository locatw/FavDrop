open FavDrop
open FavDrop.ExponentialBackoff
open FavDrop.Domain
open Microsoft.FSharp.Core.LanguagePrimitives
open System.Collections.Concurrent
open System.Configuration

[<EntryPoint>]
let main _ = 
    let storageConnectionString =
        ConfigurationManager.AppSettings.Item("StorageConnectionString")
    let queue = new ConcurrentQueue<FavoritedTweet>()
    let storage = new Storage.TableStorage(storageConnectionString, "FavDropAppLog")
    let retryConfig =
        { ExponentialBackoff.WaitTime = Int32WithMeasure(1000)
          ExponentialBackoff.MaxWaitTime = Int32WithMeasure(15 * 60 * 1000) }
    use logContext = Logging.makeContext storage retryAsync retryConfig
    let log = Logging.log logContext

    Logging.run logContext |> Async.Start

    [TwitterSource.run log queue retryAsync; DropboxSink.run log queue retryAsync]
    |> Async.Parallel
    |> Async.RunSynchronously
    |> ignore

    Logging.cancel logContext

    0
