#r @"packages\RavenDB.Client.2.5.2850\lib\net45\Raven.Client.Lightweight.dll"
#r @"packages\RavenDB.Client.2.5.2850\lib\net45\Raven.Abstractions.dll"
#load "Twitter.fs"

open System
open Raven.Client
open Raven.Client.Document
open Raven.Client.Linq
open System.Linq
open Twitter

let docStore = new DocumentStore(Url = "http://localhost:8080")

docStore.DefaultDatabase <- "RavenDB"
docStore.Initialize()

let session = docStore.OpenSession()
let maxRetweetCount = session.Query<Tweet>().Max(fun t -> t.RetweetCount)
let tweets = session.Query<Tweet>().Where(fun t -> t.RetweetCount = maxRetweetCount).Take(5).ToArray()

for tweet in tweets do
    printfn "%s\r\n" tweet.Text
