# Gossip and Push-Sum Algorithms in Distributed Systems  

## Overview  
This project implements and analyzes the **Gossip** and **Push-Sum** algorithms in a distributed system, focusing on their performance across different network topologies.  

- **Gossip Algorithm**: Spreads information (a "rumor") across nodes in a network until convergence.  
- **Push-Sum Algorithm**: Computes aggregate values over the network by stabilizing the ratio of sum (`s`) and weight (`w`) across nodes. 

## Getting Started  

### Prerequisites  
- **Editor:** [Visual Studio Code](https://code.visualstudio.com/)  
- **Programming Language:** F#  
- **Supported OS:** Windows, MacOS  
- **Dependencies:**  
  - Install [Ionide for F#](https://ionide.io/) (VS Code Plugin)  
  - Install [.NET SDK](https://dotnet.microsoft.com/download)  

### Installation & Setup  
1. Download the `Team27.zip` file and extract it.  
2. Open `program.fs` in Visual Studio Code.  
3. Open a terminal and navigate to the project folder.  
4. Install the necessary package:  
   ```sh
   dotnet add package Akka.FSharp
   ```
5. Compile the project:
    ```sh
    dotnet build
    ```
    If successful, you will see a Build succeeded message.

## Running the Program
Execute the following command:  

```sh
dotnet run <numNodes> <topology> <algorithm>
```
Parameters:  
<numNodes>: Number of nodes in the network.  
<topology>: Type of network topology. Options:  
  full    
  2D    
  line    
  imp3D    
<algorithm>: Algorithm to use. Options:    
  gossip    
  push-sum    

# Example Usage:
```sh
dotnet run 1000 full gossip
```
This runs the Gossip Algorithm on a full topology with 1000 nodes.  

## What Works?   
The project successfully implements Gossip and Push-Sum algorithms.  
The algorithms function efficiently across different network structures.  
The Gossip Algorithm reliably spreads information in a controlled manner.  
The Push-Sum Algorithm effectively computes distributed aggregates.  
