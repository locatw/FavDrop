module FavDrop.Logging

open FSharp.Control
open Microsoft.FSharp.Core.LanguagePrimitives
open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Table
open System
open System.Collections.Generic

type Severity =
| Information
| Warning
| Error
| Debug
with
    override this.ToString() =
        match this with
        | Information -> "Information"
        | Warning -> "Warning"
        | Error -> "Error"
        | Debug -> "Debug"

    static member MakeFromString(value : string) =
        match value with
        | "Information" -> Information
        | "Warning" -> Warning
        | "Error" -> Error
        | "Debug" -> Debug

type private LogRecord = {
    DateTime : DateTimeOffset
    Severity  : Severity
    Message : string
}

type LogRecordEntity (dateTime: DateTimeOffset, severity : Severity, message : string) as this =
    inherit TableEntity()

    let mutable dateTime = dateTime
    let mutable severity = severity
    let mutable message = message

    do
        let utcDateTime = DateTime.UtcNow
        this.PartitionKey <-  utcDateTime.ToString("yyyy-MM")
        this.RowKey <- (DateTime.MaxValue.Ticks - utcDateTime.Ticks).ToString("d19")

    member this.DateTime
        with get () = dateTime
        and set (value) = dateTime <- value

    member this.Severity
        with get () = severity
        and set (value) = severity <- value

    member this.Message
        with get () = message
        and set (value) = message <- value

    override this.WriteEntity(operationContext : OperationContext) : IDictionary<string, EntityProperty> =
        let results = base.WriteEntity(operationContext)
        results.Add("Severity", new EntityProperty(this.Severity.ToString()))
        results

    override this.ReadEntity(properties : IDictionary<string, EntityProperty>, operationContext : OperationContext) : unit =
        base.ReadEntity(properties, operationContext)
        this.Severity <- Severity.MakeFromString(properties.["Severity"].StringValue)

type ILogger =
    abstract member Log : Severity -> string -> unit

type Logger(tableStorage : Storage.ITableStorage, retryAsync : ExponentialBackoff.RetryAsync) =
    let tableStorage = tableStorage
    let recordQueue = new BlockingQueueAgent<LogRecord>(1000)
    let cancelledEvent = new System.Threading.AutoResetEvent(false)
    let mutable disposed = false
    let mutable cancellationTokenSource = new System.Threading.CancellationTokenSource()

    let retryConfig =
        { ExponentialBackoff.WaitTime = Int32WithMeasure(1000)
          ExponentialBackoff.MaxWaitTime = Int32WithMeasure(15 * 60 * 1000) }

    let insertAsync entity = async {
        let isSuccessCode code = 200 <= code && code < 300

        try
            let! result = tableStorage.InsertAsync(entity) |> Async.AwaitTask
            match result.HttpStatusCode with
            | code when isSuccessCode code -> return ExponentialBackoff.NoRetry
            | _ -> return ExponentialBackoff.Retry
        with
        | Storage.InsertException(_) -> return ExponentialBackoff.Retry
    }
    let storeLogAsync () = async {
        try
            // specify timeout for cancelling
            let! record = recordQueue.AsyncGet(1000)
            // To avoid overlapping RowKey, create TableEntity after getting it from queue.
            let entity = new LogRecordEntity(DateTimeOffset.UtcNow, record.Severity, record.Message)
            do! retryAsync retryConfig (fun () -> insertAsync entity |> Async.RunSynchronously)
        with
        | :? System.TimeoutException -> ()
    }

    let rec storeLogLoop () = async {
        do! storeLogAsync()
        return! storeLogLoop()
    } 

    member this.Start() =
        cancellationTokenSource <- new System.Threading.CancellationTokenSource()

        Async.Start(
            async {
                try
                    return! storeLogLoop()
                finally
                    cancelledEvent.Set() |> ignore
            }, cancellationTokenSource.Token)

    member this.Cancel () =
        cancellationTokenSource.Cancel()

    member this.WaitForStop () =
        Async.AwaitWaitHandle cancelledEvent

    member this.Log severity message =
        (this :> ILogger).Log severity message

    member this.Dispose(disposing : bool) =
        match (disposed, disposing) with
        | true, _ ->
            ()
        | _, true ->
            cancellationTokenSource.Dispose()
            cancelledEvent.Dispose()
            disposed <- true
        | _, false ->
            disposed <- true
    
    interface ILogger with
        member this.Log severity (message : string) =
            let record = { DateTime = DateTimeOffset.UtcNow; Severity = severity; Message = message }
            recordQueue.Add(record)

    interface IDisposable with
        member this.Dispose() =
            this.Dispose(true)
            GC.SuppressFinalize(this)


type Log = Severity -> string -> unit

let log (logger : ILogger) severity (message : string) =
    logger.Log severity message
