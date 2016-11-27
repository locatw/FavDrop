module FavDrop.Logging.Test

open FavDrop.ExponentialBackoff
open FsUnit
open Microsoft.FSharp.Core.LanguagePrimitives
open Microsoft.WindowsAzure.Storage.Table
open NUnit.Framework
open System.Collections.Generic
open System.Threading.Tasks

[<TestFixture>]
module LoggerTest =
    let emptyStorage (getHttpStatusCode : unit -> int) (storedEntities : IList<ITableEntity>) =
        { new FavDrop.Storage.ITableStorage
          with
            member __.InsertAsync(entity) =
                storedEntities.Add(entity)
                Task.Factory.StartNew(fun () ->
                    let result = new TableResult()
                    result.HttpStatusCode <- getHttpStatusCode()
                    result) }

    let defaultRetryConfig = 
        { WaitTime = Int32WithMeasure(1)
          MaxWaitTime = Int32WithMeasure(1) }

    [<Test>]
    let ``store log message into storage once`` () =
        let getHttpStatusCode () = 200
        let storedEntities = new List<ITableEntity>()
        let emptyStorage = emptyStorage getHttpStatusCode storedEntities

        use logContext = makeContext emptyStorage retryAsync defaultRetryConfig

        run logContext |> Async.Start
        log logContext Information "test"

        Async.Sleep(100) |> Async.RunSynchronously

        cancel logContext

        storedEntities
        |> Seq.toList
        |> List.map (fun entity -> (entity :?> LogRecordEntity).Message)
        |> should equal ["test"]

    [<Test>]
    let ``cancel logging`` () =
        let getHttpStatusCode () = 200
        let storedEntities = new List<ITableEntity>()
        let emptyStorage = emptyStorage getHttpStatusCode storedEntities

        use logContext = makeContext emptyStorage retryAsync defaultRetryConfig

        run logContext |> Async.Start
        log logContext Information "test"

        Async.Sleep(100) |> Async.RunSynchronously

        cancel logContext

        log logContext Information "test2"

        storedEntities
        |> Seq.toList
        |> List.map (fun entity -> (entity :?> LogRecordEntity).Message)
        |> should equal ["test"]

    [<Test>]
    let ``insert record failed, then retry inserting`` () =
        let getHttpStatusCode =
            let codes = ref [400; 200]
            (fun () ->
                let code = List.head !codes
                codes := List.tail !codes
                code)
        let storedEntities = new List<ITableEntity>()
        let emptyStorage = emptyStorage getHttpStatusCode storedEntities

        use logContext = makeContext emptyStorage retryAsync defaultRetryConfig

        run logContext |> Async.Start
        log logContext Information "test"

        Async.Sleep(100) |> Async.RunSynchronously

        cancel logContext

        storedEntities
        |> Seq.toList
        |> List.map (fun entity -> (entity :?> LogRecordEntity).Message)
        |> should equal ["test"; "test"]
