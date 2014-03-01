module Twitter

open System

type Tweet = 
    { Id : uint64
      Text : string
      RetweetCount : int
      CreatedAt : DateTime
      Hashtags : string array
      Mentions : string array
      ScreenName : string
      Name : string }

type Follower = 
    {  
        Id: string
        ScreenName : string
        Name: string
        Location : string
        Description : string
    }
