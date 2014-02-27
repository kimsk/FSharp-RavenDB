open System
open Raven.Client
open Raven.Client.Document
open Raven.Client.Linq

type Person = {        
        FirstName : string
        LastName : string
    }

type PersonWithId = {
        mutable Id : string
        FirstName : string
        LastName : string
    }

[<EntryPoint>]
let main argv = 
    let docStore = DocumentStore.OpenInitializedStore("RavenDB")
    
    let person1 = { Person.FirstName = "Karlkim"; LastName = "Suwanmongkol";}
    let person2 = { Id = null; FirstName = "Karlkim"; LastName = "Suwanmongkol";}
    
    use session = docStore.OpenSession()
    store person1 |> run session
    store person2 |> run session
    let result = [person1;person1] |> List.map (fun p -> (store p |> run session))
    
    printfn "%A" result

    0 // return an integer exit code
