open System
open Raven.Client
open Raven.Client.Document
open Raven.Client.Linq
open System.Linq
open Twitter
open LinqToTwitter
open TwitterContext

let getStatusesByScreenName screenName batches = 
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

let toTweet (s : Status) = 
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


let toFollower (f:User) =
    {
        Id = f.ScreenName
        ScreenName = f.ScreenNameResponse
        Name = f.Name
        Location = f.Location
        Description = f.Description
    }

let saveNewFollowers (session:IDocumentSession) =
    // followers
    let followers = getAllFollowers "kimsk" |> List.map toFollower

    // store followers    
    let isMyFollower follower =
        session.Query<Follower>().Any(fun f -> f.ScreenName = follower.ScreenName)

    let newFollowers = followers |> List.filter (isMyFollower >> not)

    newFollowers |> List.iter (fun f -> 
            session.Store(f)
        )

    printfn "%d new followers" newFollowers.Length
    session.SaveChanges()


let saveNewTweets (session:IDocumentSession) tweetType allTweets = 
    let isNewTweet tweet =
        not <| session.Query<Tweet>().Any(fun t -> t.StatusId = tweet.StatusId)

    let newTweets = allTweets |> List.filter isNewTweet
        
    newTweets |> List.iter (fun t -> 
            session.Store(t)
        )

    printfn "%d new %s tweets" newTweets.Length tweetType
    session.SaveChanges()

let saveNewStatuses (session:IDocumentSession) tweetType statuses = 
    let isNewStatus (status:Status) =
        not <| session.Query<Status>().Any(fun s -> s.StatusID = status.StatusID)

    let newStatuses = statuses |> List.filter isNewStatus
        
    newStatuses |> List.iter (fun s -> 
            session.Store(s)
        )

    printfn "%d new %s tweets" newStatuses.Length tweetType
    session.SaveChanges()

[<EntryPoint>]
let main argv =
    let docStore = new DocumentStore(Url = "http://localhost:8080", DefaultDatabase = "Twitter")
    docStore.DefaultDatabase <- "Twitter"
    docStore.Initialize() |> ignore
    docStore.Conventions.MaxNumberOfRequestsPerSession <- 50000
    use session = docStore.OpenSession()

    saveNewFollowers session

    let screenName = "kimsk"
    let kimskStatuses = getStatusesByScreenName screenName 10

    kimskStatuses
        |> List.map toTweet
        |> saveNewTweets session screenName

    kimskStatuses
        |> saveNewStatuses session screenName

    let hashtag = "#fsharp"    
    let fSharpStatuses = getStatuses hashtag 100 20 UInt64.MaxValue 
    fSharpStatuses
        |> List.map toTweet
        |> saveNewTweets session hashtag

    fSharpStatuses
        |> saveNewStatuses session hashtag

    0 // return an integer exit code
