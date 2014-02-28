#r @"packages\RavenDB.Client.1.0.992\lib\net40\Raven.Client.Lightweight.dll"
#r @"packages\RavenDB.Client.1.0.992\lib\net40\Raven.Abstractions.dll"
#r @"packages\RavenDB.Client.FSharp.1.0.992\lib\net40\Raven.Client.Lightweight.FSharp.dll"
#r @"packages\NLog.2.0.0.2000\lib\net40\NLog.dll"
#r @"packages\Newtonsoft.Json.4.0.8\lib\net40\Newtonsoft.Json.dll"
#load "Twitter.fs"

open System
open Raven.Client
open Raven.Client.Document
open Raven.Client.Linq
open System.Linq
open Twitter

let docStore = new DocumentStore(Url = "http://localhost:8080")
docStore.DefaultDatabase <- "RavenDB"
//docStore.Conventions.CustomizeJsonSerializer <- (fun s -> s.Converters.Add(new UnionTypeConverter()))
docStore.Initialize()

let session = docStore.OpenSession()    

let tweets = session.Query<Tweet>().Take(5)

tweets.First()