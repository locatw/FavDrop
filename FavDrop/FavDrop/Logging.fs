module FavDrop.Logging

open System
open Microsoft.WindowsAzure.Storage.Table
open Microsoft.WindowsAzure.Storage
open System.Collections.Generic
open FSharp.Control

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

type Logger(tableStorage : Storage.ITableStorage) =
    let tableStorage = tableStorage
    let recordQueue = new BlockingQueueAgent<LogRecord>(1000)

    member this.Start() =
        async {
            while true do
                let! record = recordQueue.AsyncGet()
                // To avoid overlapping RowKey, create TableEntity after getting it from queue.
                let entity = new LogRecordEntity(DateTimeOffset.UtcNow, record.Severity, record.Message)
                tableStorage.InsertAsync(entity)
                |> Async.AwaitTask
                |> ignore
        }
        |> Async.Start

    member this.Log severity message =
        (this :> ILogger).Log severity message
    
    interface ILogger with
        member this.Log severity (message : string) =
            let record = { DateTime = DateTimeOffset.UtcNow; Severity = severity; Message = message }
            recordQueue.Add(record)

type Log = Severity -> string -> unit

let log (logger : ILogger) severity (message : string) =
    logger.Log severity message
