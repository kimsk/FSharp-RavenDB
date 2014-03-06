open System
open Raven.Client
open Raven.Client.Document
open Raven.Client.Linq
open System.Linq
open Twitter
open LinqToTwitter
open TwitterContext

let getTweetsByScreenName screenName batches = 
    let getTweets maxId = 
        let tweets = 
            let q = ctx.Status.Where(fun s -> s.Type = StatusType.User && s.ScreenName = screenName && s.Count = 200)
            if maxId <> UInt64.MaxValue then q.Where(fun s -> s.MaxID = maxId)
            else q
        tweets
        |> List.ofSeq
        |> List.rev
    
    let getAllTweets (acc : Status list) _ = 
        let maxId = 
            if acc = [] then UInt64.MaxValue
            else 
                (acc
                 |> List.head
                 |> (fun s -> s.StatusID)) - 1UL
        
        let tweets = (getTweets maxId) @ acc
        printfn "total: %d maxId: %d" tweets.Length maxId
        tweets
    
    [ 0..batches ] |> List.fold getAllTweets []

let getHashtags (s : Status) = 
    s.Entities.HashTagEntities
    |> Seq.map (fun x -> x.Tag.ToLower())
    |> Array.ofSeq

let getMentions (s : Status) = 
    s.Entities.UserMentionEntities
    |> Seq.map (fun x -> x.ScreenName.ToLower())
    |> Array.ofSeq

let getTweet (s : Status) = 
    { StatusId = s.StatusID      
      Text = s.Text
      RetweetCount = s.RetweetCount
      CreatedAt = s.CreatedAt
      Hashtags = getHashtags s
      Mentions = getMentions s
      ScreenName = s.User.ScreenNameResponse
      Name = s.User.Name }

let getAllFollowers screenName = 
    
    let rec getAllFollowers' acc nextCursor =
        match nextCursor with
        | 0L -> acc
        | next -> 
            let newFriendship = 
                    ctx.Friendship
                        .Where(fun f -> f.Type = FriendshipType.FollowersList && f.ScreenName = screenName && f.Count = 200 && f.Cursor = next)
                        .Single()
            let newFollowers = (newFriendship.Users |> List.ofSeq) @ acc
            let nextCursor = (newFriendship.CursorMovement.Next)
            printfn "total: %d nextCursor: %d" newFollowers.Length nextCursor
            getAllFollowers' newFollowers nextCursor

    getAllFollowers' [] -1L

// get search result with specific maxId
let getSearchResultWithMaxId q num maxId = 
    query { 
        for searchResult in ctx.Search do
            where (searchResult.Type = SearchType.Search)
            where (searchResult.Query = q)
            where (searchResult.Count = num)
            where (searchResult.MaxID = maxId)
            select searchResult
            exactlyOne
    }

let getSearchResultWithSinceId q num sinceId = 
    query { 
        for searchResult in ctx.Search do
            where (searchResult.Type = SearchType.Search)
            where (searchResult.Query = q)
            where (searchResult.Count = num)
            where (searchResult.SinceID = sinceId)
            select searchResult
            exactlyOne
    }
 
// get statuses from number of batches
let getStatuses q num batches lastMaxId = 
    
    let getStatuses q maxId = 
        (getSearchResultWithMaxId q num maxId).Statuses
        |> List.ofSeq
        |> List.rev
    
    let combinedStatuses (acc : Status list) _ = 
        let maxId = 
            if acc = [] then lastMaxId
            else 
                (acc
                 |> List.head
                 |> (fun s -> s.StatusID)) - 1UL
        (getStatuses q maxId) @ acc
    
    [ 0..batches ] |> List.fold combinedStatuses []

let getAllTweetsFromRavenDB (session:IDocumentSession) q =
    let rec getTweets (acc:list<Tweet>) count =   
        let total = acc.Count()     
        match count with
        | 0 -> acc
        | _ -> 
            let tweets = session.Advanced.LuceneQuery<Tweet>().Where("NOT ScreenName : kimsk AND Hashtags:" + q).Skip(total).ToList() |> List.ofSeq
            printfn "%d %d" count (tweets.Count())
            getTweets (tweets@acc) (tweets.Count())

    getTweets [] 1


let addNewFollowers (session:IDocumentSession) =
    // followers
    let followers = getAllFollowers "kimsk" |> List.map (fun f -> 
                                            {
                                                Id = f.ScreenName
                                                ScreenName = f.ScreenNameResponse
                                                Name = f.Name
                                                Location = f.Location
                                                Description = f.Description
                                            })

    // store followers    
    let isMyFollower screenName =
        session.Query<Follower>().Any(fun f -> f.ScreenName = screenName)

    let addIfNewFollower acc follower =
        if isMyFollower follower.ScreenName then
            acc
        else
            follower::acc

    let newFollowers = followers |> List.fold addIfNewFollower []
   
    for follower in newFollowers do       
        session.Store(follower)
    
    printfn "%d new followers" newFollowers.Length
    session.SaveChanges()

 
let addNewTweets (session:IDocumentSession) =
    // add new tweets since last tweet
    let lastTweet = session.Query<Tweet>().OrderByDescending(fun t -> t.CreatedAt).First()
    let lastStatusId = lastTweet.StatusId
    let q = 
        ctx.Status.Where(fun s -> s.Type = StatusType.User && s.ScreenName = "kimsk" && s.Count = 200 && s.SinceID = lastStatusId)
        |> Seq.map getTweet
        |> Seq.sortBy (fun t -> t.CreatedAt)
        
    printfn "%A" (q |> Seq.head)

    for tweet in q do
        session.Store(tweet)

    session.SaveChanges()

let addNewFSharpTweets (session:IDocumentSession) =
    let allFSharpTweets = getAllTweetsFromRavenDB session "fsharp"
    let fsharpSinceId = allFSharpTweets.Max(fun t -> t.StatusId)
    printfn "%d %A" (allFSharpTweets.Count()) (allFSharpTweets.Where(fun t -> t.StatusId = fsharpSinceId))

    printfn "%A" ((getSearchResultWithSinceId "#fsharp" 1 (fsharpSinceId - 1000UL)).Statuses.First().Text)
    
    let fsharpTweets = getStatuses "#fsharp" 100 20 UInt64.MaxValue |> Seq.map getTweet
    printfn "%A" fsharpTweets

    for tweet in fsharpTweets do
        if session.Query<Tweet>().Any(fun t -> t.StatusId = tweet.StatusId) then
            printfn "Duplicated tweet : %d %s" tweet.StatusId tweet.Text
        else
            printfn "Add tweet : %d %s" tweet.StatusId tweet.Text
            session.Store(tweet)

    session.SaveChanges()

[<EntryPoint>]
let main argv =
    let docStore = new DocumentStore(Url = "http://localhost:8080", DefaultDatabase = "Twitter")
    docStore.DefaultDatabase <- "Twitter"
    docStore.Initialize() |> ignore
    docStore.Conventions.MaxNumberOfRequestsPerSession <- 5000
    use session = docStore.OpenSession()

    addNewFollowers session
    addNewTweets session
    addNewFSharpTweets session
    0 // return an integer exit code
