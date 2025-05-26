module Chord
#r "nuget: Akka.FSharp"

open Akka
open Akka.FSharp
open Akka.Actor
open System

open System
open Akka.FSharp

// Hop counter 
type HopMessage =
    | IncNodeConverged of int * int * int

//Calculates and returns maximum key if a chord ring and m-bit identifier based on the numNodes in the chord ring
let rec calculateMaxNumHelper n m =
    if n>0 then
        calculateMaxNumHelper (n>>>1)(m+1)
    else
        1<<<m, m
// hopCounter function measures hop counts and handles convergence of nodes.
let hopCounter numNodes (mailbox: Actor<_>) = 
    // Mutable variables to keep track of hop counts, total requests, and converged nodes.
    let mutable totHopCount = 0
    let mutable totNumRequest = 0
    let mutable totConvergedNodes = 0
    // Recursive function to handle incoming messages.
    let rec hopCounterLoop () = actor {
        let! message = mailbox.Receive ()
        match message with
        | IncNodeConverged (nodeID, hopCount, numRequest) ->
            totHopCount <- totHopCount + hopCount
            totNumRequest <- totNumRequest + numRequest
            totConvergedNodes <- totConvergedNodes + 1
            if(totConvergedNodes = numNodes) then
                printfn "Total num of hops: %d" totHopCount
                printfn "Total reqs * no of nodes: %d" totNumRequest
                printfn "Avg hops: %f" ((float totHopCount) / (float totNumRequest))
                mailbox.Context.System.Terminate() |> ignore
        // Process incoming messages
        return! hopCounterLoop ()
    }
    hopCounterLoop ()

// Node 
type NodeMessage =
    | Create
    | Join of int
    | SearchForSuccessor of int
    | UpdateSuccessor of int
    | MaintainStability
    | RequestPredecessor
    | UpdatePredecessor of int
    | Notify of int
    | AdjustFingers
    | QueryFingerSuccessor of int * int * int
    | UpdateFinger of int * int   
    | InitiateQuery
    | GenerateRandomKeyQuery
    | SearchForKeySuccessor of int * int * int
    | KeyLookupResult of int
// Construct an actor path for a given node ID.
let actorPathForID s =
    let pathActor = @"akka://my-system/user/" + string s
    pathActor
// Checks if a given value falls within an interval on a circular ring.
let intervalCheck maxKey left value right inclusiveRight =
    let adjustRight = if right < left then right + maxKey else right
    let adjustValue = if value < left && left > right then value + maxKey else value

    if inclusiveRight then
        (left = right) || (adjustValue > left && adjustValue <= adjustRight)
    else
        (left = right) || (adjustValue > left && adjustValue < adjustRight)
// Define an actor for a Chord node with a specified node ID
let nodeActor (nodeID: int) m maxKey maxNumRequests hopsRef (mailbox: Actor<_>) =
    printfn "NodeID: %d" nodeID
    let mutable predID = -1
    let mutable fingerTable = Array.create m -1
    let mutable next = 0
    let mutable totalHops = 0
    let mutable numReqs = 0
    // processMessage function defines an actor to handle messages for a Chord node
    let rec processMessage () = actor {
        let! message = mailbox.Receive ()
        let sender = mailbox.Sender ()
        match message with
        // Handle message: Initialize a new Chord node.
        | Create ->
            predID <- -1
            for i = 0 to m - 1 do
                fingerTable.[i] <- nodeID
            // - Schedule stability and finger adjustment tasks.
            mailbox.Context.System.Scheduler.ScheduleTellRepeatedly (
                TimeSpan.FromMilliseconds(0.0),
                TimeSpan.FromMilliseconds(500.0),
                mailbox.Self,
                MaintainStability
            )
            mailbox.Context.System.Scheduler.ScheduleTellRepeatedly (
                    TimeSpan.FromMilliseconds(0.0),
                    TimeSpan.FromMilliseconds(500.0),
                    mailbox.Self,
                    AdjustFingers
                )
        //Join an existing Chord network.  
        | Join (targetNodeID) ->
            predID <- -1
            let targetNodePath = actorPathForID targetNodeID
            let targetNodeRef = mailbox.Context.ActorSelection targetNodePath
            targetNodeRef <! SearchForSuccessor (nodeID)
        // Search for the successor of a given key.
        | SearchForSuccessor (id) ->
            if(intervalCheck maxKey nodeID id fingerTable.[0] true) then
                let successorNodePath = actorPathForID id
                let successorNodeRef = mailbox.Context.ActorSelection successorNodePath
                successorNodeRef <! UpdateSuccessor (fingerTable.[0])
            else
                let mutable i = m - 1
                while(i >= 0) do
                    if(intervalCheck maxKey nodeID fingerTable.[i] id false) then
                        let candidatePredecessorID = fingerTable.[i]
                        let candidatePredecessorPath = actorPathForID candidatePredecessorID
                        let candidatePredecessorRef = mailbox.Context.ActorSelection candidatePredecessorPath
                        candidatePredecessorRef <! SearchForSuccessor (id)
                        i <- -1
                    i <- i - 1
        // Update the successor node.
        | UpdateSuccessor (successorID) ->
            for i = 0 to m - 1 do
                fingerTable.[i] <- successorID
            // initialize performing stabilization and adjust fingers schedulers
            mailbox.Context.System.Scheduler.ScheduleTellRepeatedly (
                TimeSpan.FromMilliseconds(0.0),
                TimeSpan.FromMilliseconds(500.0),
                mailbox.Self,
                MaintainStability
            )
            mailbox.Context.System.Scheduler.ScheduleTellRepeatedly (
                    TimeSpan.FromMilliseconds(0.0),
                    TimeSpan.FromMilliseconds(500.0),
                    mailbox.Self,
                    AdjustFingers
                )
        // Handle message: Maintain stability by checking the immediate successor.
        | MaintainStability ->
            let successorID = fingerTable.[0]
            let successorPath = actorPathForID successorID
            let successorRef = mailbox.Context.ActorSelection successorPath
            successorRef <! RequestPredecessor
        // Request predecessor information from the successor.
        | RequestPredecessor ->
            sender <! UpdatePredecessor (predID)  // - Send the current predecessor's ID to the sender.
        // Update the predecessor based on new information.
        | UpdatePredecessor (x) ->
            if((x <> -1) && (intervalCheck maxKey nodeID x fingerTable.[0] false)) then
                fingerTable.[0] <- x
            let successorID = fingerTable.[0]
            let successorPath = actorPathForID successorID
            let successorRef = mailbox.Context.ActorSelection successorPath
            successorRef <! Notify (nodeID)
        // Notify the node about a new potential predecessor.
        | Notify (nDash) ->
            if((predID = -1) || (intervalCheck maxKey predID nDash nodeID false)) then
                predID <- nDash
        // Adjust the fingers to improve routing efficiency.
        | AdjustFingers ->
            next <- next + 1
            if(next >= m) then
                next <- 0
            let fingerValue = nodeID + int (Math.Pow(2.0, float (next)))
            mailbox.Self <! QueryFingerSuccessor (nodeID, next, fingerValue)
        // Query for the successor of a specific finger.
        | QueryFingerSuccessor (requestingNodeID, next, id) ->
            if(intervalCheck maxKey nodeID id fingerTable.[0] true) then
                let originNodePath = actorPathForID requestingNodeID
                let originNodeRef = mailbox.Context.ActorSelection originNodePath
                originNodeRef <! UpdateFinger (next, fingerTable.[0])
            else
                let mutable i = m - 1
                while(i >= 0) do
                    if(intervalCheck maxKey nodeID fingerTable.[i] id false) then
                        let candidatePredecessorID = fingerTable.[i]
                        let candidatePredecessorPath = actorPathForID candidatePredecessorID
                        let candidatePredecessorRef = mailbox.Context.ActorSelection candidatePredecessorPath
                        candidatePredecessorRef <! QueryFingerSuccessor (requestingNodeID, next, id)
                        i <- -1
                    i <- i - 1
         // Update a specific finger with a new successor.
        | UpdateFinger (next, fingerSuccessor) ->
            fingerTable.[next] <- fingerSuccessor
        // Initiate random key lookup queries.
        | InitiateQuery ->
            if(numReqs < maxNumRequests) then
                mailbox.Self <! GenerateRandomKeyQuery
                mailbox.Context.System.Scheduler.ScheduleTellOnce(TimeSpan.FromSeconds(1.), mailbox.Self, InitiateQuery)
            else
                // All queries completed. Send the final status to the hop counter for aggregation.
                hopsRef <! IncNodeConverged (nodeID, totalHops, numReqs)
        // Generate a key randomly for a query.
        | GenerateRandomKeyQuery ->
            let key = (System.Random()).Next(maxKey)
            mailbox.Self <! SearchForKeySuccessor (nodeID, key, 0)

        // Scalable key lookup
        | SearchForKeySuccessor (requestingNodeID, id, numHops) ->
            if(id = nodeID) then
                // Print the result of a key lookup, indicating that the key is found at 'nodeID'
                let originNodePath = actorPathForID requestingNodeID
                let originNodeRef = mailbox.Context.ActorSelection originNodePath
                originNodeRef <! KeyLookupResult (numHops)
            elif(intervalCheck maxKey nodeID id fingerTable.[0] true) then
                // Print the result for a successful key lookup showing the key, the responsible node, and the hop count.
                let originNodePath = actorPathForID requestingNodeID
                let originNodeRef = mailbox.Context.ActorSelection originNodePath
                originNodeRef <! KeyLookupResult (numHops)
            else
                let mutable i = m - 1
                while(i >= 0) do
                    if(intervalCheck maxKey nodeID fingerTable.[i] id false) then
                        let candidatePredecessorID = fingerTable.[i]
                        let candidatePredecessorPath = actorPathForID candidatePredecessorID
                        let candidatePredecessorRef = mailbox.Context.ActorSelection candidatePredecessorPath
                        candidatePredecessorRef <! SearchForKeySuccessor (requestingNodeID, id, numHops + 1)
                        i <- -1
                    i <- i - 1
        //Process the result of a key lookup.
        | KeyLookupResult (hopCount) ->
            if(numReqs < maxNumRequests) then
                totalHops <- totalHops + hopCount
                numReqs <- numReqs + 1

        return! processMessage ()
    }
    processMessage ()