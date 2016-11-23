module Storage

open Microsoft.WindowsAzure.Storage.Table
open Microsoft.WindowsAzure.Storage

exception InsertException of string * ITableEntity

type ITableStorage =
    abstract member InsertAsync : ITableEntity -> System.Threading.Tasks.Task<TableResult>

type TableStorage (connectionString : string, tableName : string) as this =
    let connectionString = connectionString
    let tableName = tableName
    let client =
        let account = CloudStorageAccount.Parse(connectionString)
        account.CreateCloudTableClient()
    let table = client.GetTableReference(tableName)
    do
        table.CreateIfNotExists() |> ignore

    member this.InsertAsync(record : ITableEntity) =
        (this :> ITableStorage).InsertAsync record

    interface ITableStorage with
        member __.InsertAsync record =
            try
                let insertOperation = TableOperation.Insert(record)
                table.ExecuteAsync(insertOperation)
            with
            | :? System.Exception as e -> raise (InsertException(e.ToString(), record))
