module FavDrop.Logging

open FSharp.Control
open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Table
open System
open System.Collections.Concurrent
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

type LogRecordEntity (dateTime: DateTimeOffset, severity : Severity, message : string) as this =
    inherit TableEntity()

    let mutable dateTime = dateTime
    let mutable severity = severity
    let mutable message = message

    do
        let utcDateTime = DateTime.UtcNow
        this.PartitionKey <-  utcDateTime.ToString("yyyy-MM")
        this.RowKey <- (DateTime.MaxValue.Ticks - utcDateTime.Ticks).ToString("d19")

    member __.DateTime
        with get () = dateTime
        and set (value) = dateTime <- value

    member __.Severity
        with get () = severity
        and set (value) = severity <- value

    member __.Message
        with get () = message
        and set (value) = message <- value

    override this.WriteEntity(operationContext : OperationContext) : IDictionary<string, EntityProperty> =
        let results = base.WriteEntity(operationContext)
        results.Add("Severity", new EntityProperty(this.Severity.ToString()))
        results

    override this.ReadEntity(properties : IDictionary<string, EntityProperty>, operationContext : OperationContext) : unit =
        base.ReadEntity(properties, operationContext)
        this.Severity <- Severity.MakeFromString(properties.["Severity"].StringValue)

type ILogContext =
    inherit IDisposable

type Log = Severity -> string -> unit

type private LogRecord = {
    DateTime : DateTimeOffset
    Severity  : Severity
    Message : string
}

[<NoComparison>]
[<NoEquality>]
type private LogContext =
    {
        Storage : Storage.ITableStorage
        RecordQueue : ConcurrentQueue<LogRecord>
        RetryAsync : ExponentialBackoff.RetryAsync
        RetryConfig : ExponentialBackoff.RetryConfig
        CancellationTokenSource : System.Threading.CancellationTokenSource
        mutable Disposed : bool
    }
    interface ILogContext
    interface IDisposable with
        member this.Dispose() =
            if this.Disposed then
                ()
            else
                this.CancellationTokenSource.Dispose()
                this.Disposed <- true
            GC.SuppressFinalize(this)

let private insert context entity =
    async {
        let isSuccessCode code = 200 <= code && code < 300

        try
            let! result = context.Storage.InsertAsync(entity) |> Async.AwaitTask
            match result.HttpStatusCode with
            | code when isSuccessCode code -> return ExponentialBackoff.NoRetry
            | _ -> return ExponentialBackoff.Retry
        with
        | Storage.InsertException(_) -> return ExponentialBackoff.Retry
    }

let private storeLog context =
    async {
        try
            let (isSucceeded, record) = context.RecordQueue.TryDequeue()
            match isSucceeded with
            | true ->
                // To avoid overlapping RowKey, create TableEntity after getting it from queue.
                let entity = new LogRecordEntity(DateTimeOffset.UtcNow, record.Severity, record.Message)
                do! context.RetryAsync context.RetryConfig (fun () -> insert context entity)
            | false ->
                do! Async.Sleep(1000)
        with
        | :? System.TimeoutException ->
            ()
    }

let rec private storeLogLoop context =
    async {
        match context.CancellationTokenSource.IsCancellationRequested with
        | true -> return ()
        | false ->
            do! storeLog context
            return! storeLogLoop context
    }

let private checkContextArgument (context : ILogContext) =
    if not (context :? LogContext) then
        raise (System.ArgumentException("context must be a returned value by makeContext"))

let makeContext storage retryAsync retryConfig =
    { Storage = storage
      RecordQueue = new ConcurrentQueue<LogRecord>()
      RetryAsync = retryAsync
      RetryConfig = retryConfig
      CancellationTokenSource = new System.Threading.CancellationTokenSource()
      Disposed = false }
    :> ILogContext

let run (context : ILogContext) =
    checkContextArgument context

    async {
        let! result = storeLogLoop (context :?> LogContext)
        return result
    }

let cancel (context : ILogContext) =
    checkContextArgument context

    let ctx = context :?> LogContext
    ctx.CancellationTokenSource.Cancel()

let log (context : ILogContext) severity message =
    checkContextArgument context

    let ctx = context :?> LogContext
    let record = { DateTime = DateTimeOffset.UtcNow; Severity = severity; Message = message }
    ctx.RecordQueue.Enqueue(record)

