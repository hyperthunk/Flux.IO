namespace Flux.IO

module Collections =

    open FSharp.HashCollections

    module FastMap = 

        let inline isEmpty (map: HashMap<_,_>) = HashMap.isEmpty map
        let inline count (map: HashMap<_,_>) = HashMap.count map
        let inline find (key: string) (map: HashMap<_,_>) = 
            match HashMap.tryFind key map with
            | ValueSome value -> value
            | ValueNone -> failwithf "Key not found: %s" key

        let inline fold<'Key, 'Value, 'State> 
                (folder: 'State -> 'Key -> 'Value -> 'State)
                (initialState: 'State)
                (table: HashMap<'Key, 'Value>) : 'State = 
            HashMap.toSeq table 
            |> Seq.map (fun struct (k, v) -> (k, v))
            |> Seq.fold (fun (s: 'State) (k, v) -> folder s k v) initialState

        let inline ofSeq<'Key, 'Value when 'Key : comparison> 
                (seq: seq<'Key * 'Value>) : HashMap<'Key, 'Value> =
            let table : HashMap<'Key, 'Value> = HashMap.empty
            seq |> Seq.fold (fun acc (k, v) -> HashMap.add k v acc) table
