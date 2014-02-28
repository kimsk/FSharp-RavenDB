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

let tweets = session.Query<Tweet>().Take(5).ToArray()
    
for tweet in tweets do printfn "%s\r\n" tweet.Text