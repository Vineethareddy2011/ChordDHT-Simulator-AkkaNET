open System
open Akka.FSharp
open System.Threading

open Chord

[<EntryPoint>]
let main argv =
    // Create the actor system to manage Chord nodes and hop counting.
    let system = System.create "my-system" (Configuration.load())

    // Parse and handle command line arguments
    let numNodes, numRequests =
        if argv.Length = 2 then
            (int argv.[0],int argv.[1])
        else
            printfn "The input is invalid. Please provide two arguments: numNodes and numRequests."
            exit 1
            (0,0)

    // Calculate the maximum key of the Chord ring based on the number of nodes in the ring.
    let m = 0
    let (maxNum, m) = calculateMaxNumHelper numNodes m

    // Spawn the actor hopCounter
    let hopCounterRef = spawn system "hopCounter" (hopCounter numNodes)

    // Spawn chord nodes
    let nodeIDs = Array.create numNodes -1;
    let nodeRefs = Array.create numNodes null;
    let mutable i = 0;
    printfn "Node creation initiated"
    while(i < numNodes) do
        try
            let nodeID  = (Random()).Next(maxNum)
            nodeIDs.[i] <- nodeID
            nodeRefs.[i] <- spawn system (string nodeID) (nodeActor nodeID m maxNum numRequests hopCounterRef)
            if(i = 0) then
                nodeRefs.[i] <! Create
            else
                nodeRefs.[i] <! Join(nodeIDs.[0])
            i <- i + 1
            Thread.Sleep(500)
        with _ -> ()
    // Allow some time for the system to stabilize before initiating further actions.
    printfn "Awaiting system stabilization..."
    Thread.Sleep(1000)
    // initiate querying
    for nodeRef in nodeRefs do
        nodeRef <! InitiateQuery
        Thread.Sleep(500)

    // Wait for all the actors to terminate before completing the program.
    system.WhenTerminated.Wait()
    0 