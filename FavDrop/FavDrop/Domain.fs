module FavDrop.Domain

open System

type PhotoMedium = {
    Url : string
}

type VideoMedium = {
    ThumnailUrl : string
    VideoUrl : string
}

type Medium =
| PhotoMedium of PhotoMedium
| VideoMedium of VideoMedium
| AnimatedGifMedium of VideoMedium

type TwitterUser = {
    Id : int64 option
    Name : string
    ScreenName : string
}

type FavoritedTweet = {
    TweetId : int64
    User : TwitterUser
    CreatedAt : DateTimeOffset
    FavoritedAt : DateTimeOffset
    Text : string
    Media : Medium list
}

module TweetInfo =
    open Newtonsoft.Json
    open Newtonsoft.Json.Linq
    open Newtonsoft.Json.Serialization

    type private StringOptionConverter () =
        inherit JsonConverter()

        override __.WriteJson(writer : JsonWriter, value : Object, _ : JsonSerializer) : unit =
            let v = value :?> string option
            match v with
            | Some v ->
                let token = JToken.FromObject(v)
                token.WriteTo(writer)
            | None ->
                let token = JToken.FromObject(null)
                token.WriteTo(writer)

        override __.ReadJson(_ : JsonReader, _ : Type, _ : Object, _ : JsonSerializer) : Object =
            raise (new NotImplementedException())

        override __.CanRead with get () : bool = false

        override __.CanConvert(objectType : Type) : bool =
            objectType = typeof<string option>

    type User =
        val Id : int64
        val Name : string
        val ScreenName : string

        new(id : int64, name : string, screenName : string) =
            { Id = id; Name = name; ScreenName = screenName }

    type Media =
        val Type : string
        [<JsonConverter(typeof<StringOptionConverter>)>] val Url : string option
        [<JsonConverter(typeof<StringOptionConverter>)>] val ThumnailUrl : string option
        [<JsonConverter(typeof<StringOptionConverter>)>] val VideoUrl: string option

        new (type_ : string, url : string option, thumnailUrl : string option, videoUrl : string option) =
            { Type = type_; Url = url; ThumnailUrl = thumnailUrl; VideoUrl = videoUrl }

    // When I use FSharp.Data.JsonProvider, it does not support DateTimeOffset.
    // So use string with timezone.
    type Tweet =
        val FormatVersion : string
        val Id : int64
        val User : User
        val CreatedAt : string
        val FavoritedAt : string
        val Text : string
        val Media : Media array

        new(formatVersion: string, id : int64, user : User, createdAt : string, favoritedAt : string, text : string, media : Media array) =
            { FormatVersion = formatVersion
              Id = id
              User = user
              CreatedAt = createdAt
              FavoritedAt = favoritedAt
              Text = text
              Media = media }

        member this.ToJson () =
            let mutable contractResolver = new DefaultContractResolver()
            contractResolver.NamingStrategy <- new SnakeCaseNamingStrategy()
            
            let mutable settings = new JsonSerializerSettings()
            settings.ContractResolver <- contractResolver
            settings.Formatting <- Formatting.Indented

            JsonConvert.SerializeObject(this, settings)
