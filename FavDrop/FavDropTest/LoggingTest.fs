module FavDrop.Logging.Test

open FavDrop.ExponentialBackoff
open FsUnit
open Microsoft.WindowsAzure.Storage.Table
open NUnit.Framework
open System.Threading.Tasks

[<TestFixture>]
module LoggerTest =
    [<Test>]
    let ``store log message into storage once`` () =
        let mutable storedEntities = []
        let emptyStorage =
            { new FavDrop.Storage.ITableStorage
              with
                member __.InsertAsync(entity) =
                    storedEntities <- entity :: storedEntities
                    Task.Factory.StartNew(fun () ->
                        let result = new TableResult()
                        result.HttpStatusCode <- 200
                        result) }

        use logger = new Logger(emptyStorage, retryAsync)
        logger.Start()
        logger.Log Information "test"
        Async.Sleep(100) |> Async.RunSynchronously
        logger.Cancel()

        Async.RunSynchronously (logger.WaitForStop() |> Async.Ignore)

        storedEntities
        |> List.map (fun entity -> (entity :?> LogRecordEntity).Message)
        |> should equal ["test"]


