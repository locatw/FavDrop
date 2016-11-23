open ExponentialBackoff
open FavDrop.Domain
open FSharp.Control
open Microsoft.Azure
open System
open System.Configuration

        
[<EntryPoint>]
let main _ = 
    let storageConnectionString =
        CloudConfigurationManager.GetSetting("StorageConnectionString")
    let queue = new BlockingQueueAgent<FavoritedTweet>(100)
    let storage = new Storage.TableStorage(storageConnectionString, "FavDropAppLog")
    let logger = new Logging.Logger(storage)
    let log = Logging.log logger

    logger.Start()

    [TwitterSource.run log queue retryAsync; DropboxSink.run log queue]
    |> Async.Parallel
    |> Async.RunSynchronously
    |> ignore
    
    0
