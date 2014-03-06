#r @"packages\RavenDB.Client.2.5.2850\lib\net45\Raven.Client.Lightweight.dll"
#r @"packages\RavenDB.Client.2.5.2850\lib\net45\Raven.Abstractions.dll"
#load "Twitter.fs"

open System
open Raven.Client
open Raven.Client.Document
open Raven.Client.Linq
open System.Linq
open Twitter

let docStore = new DocumentStore(Url = "http://localhost:8080", DefaultDatabase = "Twitter")
docStore.Initialize()
docStore.Conventions.MaxNumberOfRequestsPerSession <- 5000
let session = docStore.OpenSession()
let maxRetweetCount = session.Query<Tweet>().Max(fun t -> t.RetweetCount)
let tweets = session.Query<Tweet>().Where(fun t -> t.RetweetCount = maxRetweetCount).Take(5).ToArray()

for tweet in tweets do
    printfn "%s\r\n" tweet.Text

let followers = session.Query<Follower>() |> Array.ofSeq

followers |> Seq.where (fun f -> f.ScreenName = "nashdotnet") 

// non retweets
let nonRetweets = 
    session.Query<Tweet>()
        .Where(fun t -> not(t.Text.StartsWith("RT")))
        .OrderByDescending(fun t -> t.RetweetCount)

nonRetweets.First().Text

let lastTweet = session.Query<Tweet>().OrderByDescending(fun t -> t.CreatedAt).First()
lastTweet.StatusId, lastTweet.Text

let checkStatusId = 81810884602757120UL
let hasTweet = session.Query<Tweet>().Any(fun t -> t.StatusId = checkStatusId)

let getAllTweetsFromRavenDB q =
    let rec getTweets (acc:list<Tweet>) count =   
        let total = acc.Count()     
        match count with
        | 0 -> acc
        | _ -> 
            let tweets = session.Advanced.LuceneQuery<Tweet>().Where("NOT ScreenName : kimsk AND Hashtags:" + q).Skip(total).ToList() |> List.ofSeq
            printfn "%d %d" count (tweets.Count())
            getTweets (tweets@acc) (tweets.Count())

    getTweets [] 1

let allFSharpTweets = getAllTweetsFromRavenDB "fsharp"
allFSharpTweets.Count()

// if follower exist
session.Query<Follower>().Any(fun f -> f.ScreenName = "nashdotnet")

let isMyFollower screenName =
    session.Query<Follower>().Any(fun f -> f.ScreenName = screenName)


