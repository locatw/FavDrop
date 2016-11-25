module FavDrop.Logging.Test

open FavDrop.ExponentialBackoff
open FsUnit
open Microsoft.FSharp.Core.LanguagePrimitives
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

        let retryConfig =
            { WaitTime = Int32WithMeasure(1)
              MaxWaitTime = Int32WithMeasure(1) }
        use logContext = makeContext emptyStorage retryAsync retryConfig

        run logContext |> Async.Start
        log logContext Information "test"

        Async.Sleep(100) |> Async.RunSynchronously

        cancel logContext

        storedEntities
        |> List.map (fun entity -> (entity :?> LogRecordEntity).Message)
        |> should equal ["test"]


