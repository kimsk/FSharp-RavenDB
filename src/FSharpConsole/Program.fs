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
    { Id = s.StatusID
      Text = s.Text
      RetweetCount = s.RetweetCount
      CreatedAt = s.CreatedAt
      Hashtags = getHashtags s
      Mentions = getMentions s
      ScreenName = s.User.ScreenNameResponse
      Name = s.User.Name }

[<EntryPoint>]
let main argv = 
    let docStore = new DocumentStore(Url = "http://localhost:8080")
    docStore.DefaultDatabase <- "RavenDB"
    docStore.Initialize() |> ignore
    docStore.Conventions.MaxNumberOfRequestsPerSession <- 5000
    use session = docStore.OpenSession()
    
    let myTweets = 
        getTweetsByScreenName "kimsk" 20
        |> Seq.map getTweet

    printfn "%A" myTweets
        // store tweets
    for tweet in myTweets do
        session.Store(tweet)
        session.SaveChanges()
    
    // query tweets
    let tweets = session.Query<Tweet>().Take(5).ToArray()
    for tweet in tweets do
        printfn "%s" tweet.Text

    // followers
    let followers = 
        ctx.Friendship
            .Where(fun f -> f.Type = FriendshipType.FollowersList && f.ScreenName = "kimsk" && f.Count = 200)
            .Single()
            
    
    printfn "%A %A" (followers.Users |> Seq.map (fun f -> f.ScreenNameResponse)) (followers.CursorMovement.Next)             

    let followers' = 
        ctx.Friendship
            .Where(fun f -> f.Type = FriendshipType.FollowersList && f.ScreenName = "kimsk" && f.Count = 200 && f.Cursor = followers.CursorMovement.Next)
            .Single()

    printfn "%A %A" (followers'.Users |> Seq.map (fun f -> f.ScreenNameResponse)) (followers'.CursorMovement.Next)                

    0 // return an integer exit code
