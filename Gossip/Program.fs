open System
open Deedle
open Akka.FSharp

// Utility functions //

// Round number of nodes to get perfect square in case of 2D
let roundNodes numOfNodes topology =
    match topology with
    | "2D"
    | "imp3D" -> Math.Pow (Math.Round ((float numOfNodes) ** (1.0 / 3.0)), 3.0)  |> int
    | _ -> numOfNodes

// Select random element from a list
let selectRandom (l: List<_>) =
    let r = Random()
    l.[r.Next(l.Length)]

let getRandomNeighborID (topologyMap: Map<_, _>) nodeID =
    let (neighborList: List<_>) = (topologyMap.TryFind nodeID).Value
    let random = Random()
    neighborList.[random.Next(neighborList.Length)]


// Different topologies //

let buildLineTopology numOfNodes =
    let mutable map = Map.empty
    [ 1 .. numOfNodes ]
    |> List.map (fun nodeID ->
        let listNeighbors = List.filter (fun y -> (y = nodeID + 1 || y = nodeID - 1)) [ 1 .. numOfNodes ]
        map <- map.Add(nodeID, listNeighbors))
    |> ignore
    map

// Find neighbors of any particular node in a 2D grid
let gridNeighbors2D nodeID numOfNodes =
    let mutable map = Map.empty
    let lenSide = sqrt (float numOfNodes) |> int
    [ 1 .. numOfNodes ]
    |> List.filter (fun y ->
        if (nodeID % lenSide = 0) then (y = nodeID - 1 || y = nodeID - lenSide || y = nodeID + lenSide)
        elif (nodeID % lenSide = 1) then (y = nodeID + 1 || y = nodeID - lenSide || y = nodeID + lenSide)
        else (y = nodeID - 1 || y = nodeID + 1 || y = nodeID - lenSide || y = nodeID + lenSide))

let build2DTopology numOfNodes =
    let mutable map = Map.empty
    [ 1 .. numOfNodes ]
    |> List.map (fun nodeID ->
        let listNeighbors = gridNeighbors2D nodeID numOfNodes
        map <- map.Add(nodeID, listNeighbors))
    |> ignore
    map


// Find neighbors of any particular node in a 3D grid
let gridNeighbors3D nodeID numOfNodes =
    let lenSide = Math.Round(Math.Pow((float numOfNodes), (1.0 / 3.0))) |> int
    [ 1 .. numOfNodes ]
    |> List.filter (fun y ->
        if (nodeID % lenSide = 0) then
            if (nodeID % (int (float (lenSide) ** 2.0)) = 0) then
                (y = nodeID - 1 || y = nodeID - lenSide || y = nodeID - int ((float (lenSide) ** 2.0)) || y = nodeID + int ((float (lenSide) ** 2.0)))
            elif (nodeID % (int (float (lenSide) ** 2.0)) = lenSide) then
                (y = nodeID - 1 || y = nodeID + lenSide || y = nodeID - int ((float (lenSide) ** 2.0)) || y = nodeID + int ((float (lenSide) ** 2.0)))
            else
                (y = nodeID - 1 || y = nodeID - lenSide || y = nodeID + lenSide || y = nodeID - int ((float (lenSide) ** 2.0)) || y = nodeID + int ((float (lenSide) ** 2.0)))        
        elif (nodeID % lenSide = 1) then
            if (nodeID % (int (float (lenSide) ** 2.0)) = 1) then
                (y = nodeID + 1 || y = nodeID + lenSide || y = nodeID - int ((float (lenSide) ** 2.0)) || y = nodeID + int ((float (lenSide) ** 2.0)))
            elif (nodeID % (int (float (lenSide) ** 2.0)) = int (float (lenSide) ** 2.0) - lenSide + 1 ) then
                (y = nodeID + 1 || y = nodeID - lenSide || y = nodeID - int ((float (lenSide) ** 2.0)) || y = nodeID + int ((float (lenSide) ** 2.0)))
            else
                (y = nodeID + 1 || y = nodeID - lenSide || y = nodeID + lenSide || y = nodeID - int ((float (lenSide) ** 2.0)) || y = nodeID + int ((float (lenSide) ** 2.0)))
        elif (nodeID % (int (float (lenSide) ** 2.0)) > 1) && (nodeID % (int (float (lenSide) ** 2.0)) < lenSide) then
            (y = nodeID - 1 || y = nodeID + 1 || y = nodeID + lenSide || y = nodeID - int ((float (lenSide) ** 2.0)) || y = nodeID + int ((float (lenSide) ** 2.0)))
        elif (nodeID % (int (float (lenSide) ** 2.0)) > int (float (lenSide) ** 2.0) - lenSide + 1) && (nodeID % (int (float (lenSide) ** 2.0)) < (int (float (lenSide) ** 2.0))) then
            (y = nodeID - 1 || y = nodeID + 1 || y = nodeID - lenSide || y = nodeID - int ((float (lenSide) ** 2.0)) || y = nodeID + int ((float (lenSide) ** 2.0)))
        else
            (y = nodeID - 1 || y = nodeID + 1 || y = nodeID - lenSide || y = nodeID + lenSide || y = nodeID - int ((float (lenSide) ** 2.0)) || y = nodeID + int ((float (lenSide) ** 2.0))))

let buildImperfect3DTopology numOfNodes =
    let mutable map = Map.empty
    [ 1 .. numOfNodes ]
    |> List.map (fun nodeID ->
        let mutable listNeighbors = gridNeighbors3D nodeID numOfNodes
        let random =
            [ 1 .. numOfNodes ]
            |> List.filter (fun m -> m <> nodeID && not (listNeighbors |> List.contains m))
            |> selectRandom
        let listNeighbors = random :: listNeighbors
        map <- map.Add(nodeID, listNeighbors))
    |> ignore
    map

let buildFullTopology numOfNodes =
    let mutable map = Map.empty
    [ 1 .. numOfNodes ]
    |> List.map (fun nodeID ->
        let listNeighbors = List.filter (fun y -> nodeID <> y) [ 1 .. numOfNodes ]
        map <- map.Add(nodeID, listNeighbors))
    |> ignore
    map

let buildTopology numOfNodes topology =
    let mutable map = Map.empty
    match topology with
    | "line" -> buildLineTopology numOfNodes
    | "2D" -> build2DTopology numOfNodes
    | "imp3D" -> buildImperfect3DTopology numOfNodes
    | "full" -> buildFullTopology numOfNodes

// Counter Actor //

type CounterMessage =
    | GossipNodeConverge
    | PushSumNodeConverge of int * float

type Result = { NumberOfNodesConverged: int; TimeElapsed: int64; }

let counter initialCount numOfNodes (filepath: string) (stopWatch: Diagnostics.Stopwatch) (mailbox: Actor<'a>) =
    let rec loop count (dataframeList: Result list) =
        actor {
            let! message = mailbox.Receive()
            match message with
            | GossipNodeConverge ->
                let newRecord = { NumberOfNodesConverged = count + 1; TimeElapsed = stopWatch.ElapsedMilliseconds; }
                if (count + 1 = numOfNodes) then
                    stopWatch.Stop()
                    printfn "Gossip Algorithm has converged in %d ms" stopWatch.ElapsedMilliseconds
                    let dataframe = Frame.ofRecords dataframeList
                    mailbox.Context.System.Terminate() |> ignore
                return! loop (count + 1) (List.append dataframeList [newRecord])
            | PushSumNodeConverge (nodeID, avg) ->
                let newRecord = { NumberOfNodesConverged = count + 1; TimeElapsed = stopWatch.ElapsedMilliseconds }
                if (count + 1 = numOfNodes) then
                    stopWatch.Stop()
                    printfn "Push Sum Algorithm has converged in %d ms" stopWatch.ElapsedMilliseconds
                    let dataframe = Frame.ofRecords dataframeList
                    mailbox.Context.System.Terminate() |> ignore
                return! loop (count + 1) (List.append dataframeList [newRecord])
        }
    loop initialCount []


// Gossip Actor //

let gossip maxCount (topologyMap: Map<_, _>) nodeID counterRef (mailbox: Actor<_>) = 
    let rec loop (count: int) = actor {
        let! message = mailbox.Receive ()
        // Handle message here
        match message with
        | "heardRumor" ->
            // If the heard rumor count is zero, tell the counter that it has heard the rumor and start spreading it.
            // Else, increment the heard rumor count by 1
            if count = 0 then
                mailbox.Context.System.Scheduler.ScheduleTellOnce(
                    TimeSpan.FromMilliseconds(25.0),
                    mailbox.Self,
                    "spreadRumor"
                )
                counterRef <! GossipNodeConverge
                return! loop (count + 1)
            else
                return! loop (count + 1)
        | "spreadRumor" ->
            // Stop spreading the rumor if has an actor heard the rumor atleast 10 times
            // Else, Select a random neighbor and send message "heardRumor"
            // Start scheduler to wake up at next time step
            if count >= maxCount then
                return! loop count
            else
                let neighborID = getRandomNeighborID topologyMap nodeID
                let neighborPath = @"akka://my-system/user/worker" + string neighborID
                let neighborRef = mailbox.Context.ActorSelection(neighborPath)
                neighborRef <! "heardRumor"
                mailbox.Context.System.Scheduler.ScheduleTellOnce(
                    TimeSpan.FromMilliseconds(25.0),
                    mailbox.Self,
                    "spreadRumor"
                )
                return! loop count
        | _ ->
            printfn "Node %d has received unhandled message" nodeID
            return! loop count
    }
    loop 0


// Push sum //
 
type PushSumMessage =
    | Initialize
    | Message of float * float
    | Round

let pushSum (topologyMap: Map<_, _>) nodeID counterRef (mailbox: Actor<_>) = 
    let rec loop sNode wNode sSum wSum count isTransmitting = actor {
        if isTransmitting then
            let! message = mailbox.Receive ()
            match message with
            | Initialize ->
                mailbox.Self <! Message (float nodeID, 1.0)
                mailbox.Context.System.Scheduler.ScheduleTellRepeatedly (
                    TimeSpan.FromMilliseconds(0.0),
                    TimeSpan.FromMilliseconds(25.0),
                    mailbox.Self,
                    Round
                )
                return! loop (float nodeID) 1.0 0.0 0.0 0 isTransmitting
            | Message (s, w) ->
                return! loop sNode wNode (sSum + s) (wSum + w) count isTransmitting
            | Round ->
                // Select a random neighbor and send (s/2, w/2) to it
                // Send (s/2, w/2) to itself
                let neighborID = getRandomNeighborID topologyMap nodeID
                let neighborPath = @"akka://my-system/user/worker" + string neighborID
                let neighborRef = mailbox.Context.ActorSelection(neighborPath)
                mailbox.Self <! Message (sSum / 2.0, wSum / 2.0)
                neighborRef <! Message (sSum / 2.0, wSum / 2.0)
                // Check convergence
                // Actor is said to converged if s/w did not change
                // more than 10^-10 for 3 consecutive rounds
                if(abs ((sSum / wSum) - (sNode / wNode)) < 1.0e-10) then
                    let newCount = count + 1
                    if newCount = 10 then
                        counterRef <! PushSumNodeConverge (nodeID, sSum / wSum)
                        return! loop sSum wSum 0.0 0.0 newCount false
                    else
                        return! loop (sSum / 2.0) (wSum / 2.0) 0.0 0.0 newCount isTransmitting 
                else
                    return! loop (sSum / 2.0) (wSum / 2.0) 0.0 0.0 0 isTransmitting
    }
    loop (float nodeID) 1.0 0.0 0.0 0 true


[<EntryPoint>]
let main argv =
    let system = System.create "my-system" (Configuration.load())

    // Number of times any single node should heard the rumor before stop transmitting it
    let maxCount = 10
    
    // Parse command line arguments
    let topology = argv.[1]
    let numOfNodes = roundNodes (int argv.[0]) topology
    let algorithm = argv.[2]
    let filepath = "output/" + topology + "-" + string numOfNodes + "-" + algorithm + ".csv"
    
    // Create topology
    let topologyMap = buildTopology numOfNodes topology

    // Initialize stopwatch
    let stopWatch = Diagnostics.Stopwatch()

    // Spawn the counter actor
    let counterRef = spawn system "counter" (counter 0 numOfNodes filepath stopWatch)

    // Run an algorithm based on user input
    match algorithm with
    | "gossip" ->
        // Gossip Algorithm
        // Create desired number of workers and randomly pick 1 to start the algorithm
        let workerRef =
            [ 1 .. numOfNodes ]
            |> List.map (fun nodeID ->
                let name = "worker" + string nodeID
                spawn system name (gossip maxCount topologyMap nodeID counterRef))
            |> selectRandom
        // Start the timer
        stopWatch.Start()
        // Send message
        workerRef <! "heardRumor"

    | "push-sum" ->
        // Push Sum Algorithm
        // Initialize all the actors
        let workerRef =
            [ 1 .. numOfNodes ]
            |> List.map (fun nodeID ->
                let name = "worker" + string nodeID
                (spawn system name (pushSum topologyMap nodeID counterRef)))
        // Start the timer
        stopWatch.Start()
        // Send message
        workerRef |> List.iter (fun item -> item <! Initialize)


    // Wait till all the actors are terminated
    system.WhenTerminated.Wait()
    0 // return an integer exit code
    // Each actor will have a flag to describe its active state