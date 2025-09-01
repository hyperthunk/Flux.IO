// copy of https://github.com/fsprojects/FSharpx.Collections/blob/master/src/FSharpx.Collections.Experimental/RealTimeDeque.fs

//originally published by Julien
// original implementation taken from http://lepensemoi.free.fr/index.php/2010/02/05/real-time-double-ended-queue

//jf -- added rev, ofSeq, ofSeqC, lookup, append, appendC, update, remove, tryUpdate, tryRemove
//   -- added standardized C of 2 for singleton, empty, and ofSeq based on my reading of Okasaki

//pattern discriminators Cons, Snoc, and Nil

(*
    Flux.IO ChangeLog..

    Our use of this collection in bursty patterns, hits a certain point of fragility: 
    `check`’s rotation uses `LazyList.take c r`, which throws when n > length. 
    
    Under heavy front/very small back (precisely the rebalance trigger case), 
    r can be shorter than c (2), so take raises “not enough items in the list”.
    
    Changes: 
    
    Rotation now treats r as a possibly short stream and only consumes up 
    to c items per step. 
    
    When r runs out (common with fast front, tiny back), takeAtMost returns 
    empty instead of throwing, and the algorithm proceeds to drain f while 
    accumulating the already-consumed suffix in a.
    
    Now `check2` remains mathematically safe because each branch only takes 
    from the larger side (≥ n/2), so take i (front) or take j (back) won’t overrun.

    Asymptotic Complexity Analysis:

    The only functional change is replacing the use of `LazyList.drop` with “at most” 
    variants inside the rotation (`rotateDrop`/`rotateRev`) used by `RealTimeDeque.check`. 
    
    - Those operations still run in O(min(c, |r|)) per step with c being the small constant 
    ratio parameter (≥ 2). So each rotation step remains O(1).

    - The rebalancing (check/check2) still constructs new streams whose cost is scheduled via 
    `exec1`/`exec2`, preserving the real‑time O(1) worst‑case per deque operation (Cons, Snoc, 
    Tail, Init, Head, Last).

    - Linear operations (append, remove/update by index, etc.) remain O(n) or O(n/2) as before.

    - Amortization and streaming semantics are unchanged, since we don't alter `exec1`/`exec2`,
    nor the ways in which streams are advanced. The rotation is now more robust when the smaller 
    side has fewer than c elements.

    The net effect is that we have the same asymptotic bounds and only constant factors are affected.
*)

namespace Flux.IO.Internal

[<AutoOpen>]
module Deque = 

    open LList
    open System.Collections
    open System.Collections.Generic

    module Exceptions =
        let Empty = new System.Exception("Queue is empty") // TODO: make this a better exception

        let OutOfBounds = new System.IndexOutOfRangeException() // TODO: make this a better exception

        let KeyNotFound key =
            new System.Collections.Generic.KeyNotFoundException("The key " + string key + " was not present in the collection")

    type RealTimeDeque<'T>
        (
            c: int,
            frontLength: int,
            front: LazyList<'T>,
            streamFront: LazyList<'T>,
            rBackLength: int,
            rBack: LazyList<'T>,
            streamRBack: LazyList<'T>
        ) =

        member private this.c = c

        member private this.frontLength = frontLength

        member private this.front = front

        member private this.streamFront = streamFront

        member private this.rBackLength = rBackLength

        member private this.rBack = rBack

        member private this.streamRBack = streamRBack

        static member private length(q: RealTimeDeque<'T>) =
            q.frontLength + q.rBackLength

        static member private exec1: LazyList<'T> -> LazyList<'T> =
            function
            | LazyList.Cons(x, s) -> s
            | s -> s

        static member private exec2(x: LazyList<'T>) : LazyList<'T> =
            (RealTimeDeque.exec1 >> RealTimeDeque.exec1) x

        static member private check(q: RealTimeDeque<'T>) =

            let rec rotateDrop c f j r =

                let rec rotateRev c =
                    function
                    | LazyList.Nil, r, a ->
                        LazyList.append (LazyList.rev r) a
                    | LazyList.Cons(x, f), r, a ->
                        // SAFE: r may have fewer than c elements; use at-most variants
                        let a' = LazyList.dropAtMost c r
                        let b' = LazyList.append (LazyList.takeAtMost c r) a |> LazyList.rev
                        LazyList.cons x (rotateRev c (f, a', b'))

                if j < c then
                    // SAFE: dropAtMost handles j > length r
                    rotateRev c (f, LazyList.dropAtMost j r, LazyList.empty)
                else
                    match f with
                    | LazyList.Cons(x, f') ->
                        LazyList.cons x (rotateDrop c f' (j - c) (LazyList.dropAtMost c r))
                    | _ -> failwith "should not get there"

            let n = RealTimeDeque.length q

            if q.frontLength > q.c * q.rBackLength + 1 then
                let i = n / 2
                let j = n - i
                // Safe because front is the larger side in this branch (front >= n/2)
                let f' = LazyList.take i q.front
                let r' = rotateDrop q.c q.rBack i q.front
                new RealTimeDeque<'T>(q.c, i, f', f', j, r', r')
            elif q.rBackLength > q.c * q.frontLength + 1 then
                let j = n / 2
                let i = n - j
                // Safe because rBack is the larger side in this branch (rBack >= n/2)
                let r' = LazyList.take j q.rBack
                let f' = rotateDrop q.c q.front j q.rBack
                new RealTimeDeque<'T>(q.c, i, f', f', j, r', r')
            else
                q

        static member private check2 q =
            let n = RealTimeDeque.length q

            if q.frontLength > q.c * q.rBackLength + 1 then
                let i = n / 2
                let j = n - i
                let f' = LazyList.take i q.front
                let r' = LazyList.drop i q.front |> LazyList.rev |> LazyList.append q.rBack
                new RealTimeDeque<'T>(q.c, i, f', f', j, r', r')
            elif q.rBackLength > q.c * q.frontLength + 1 then
                let j = n / 2
                let i = n - j
                let r' = LazyList.take j q.rBack
                let f' = LazyList.drop j q.rBack |> LazyList.rev |> LazyList.append q.front
                new RealTimeDeque<'T>(q.c, i, f', f', j, r', r')
            else
                new RealTimeDeque<'T>(q.c, q.frontLength, q.front, q.front, q.rBackLength, q.rBack, q.rBack)

        static member internal AppendC cC (xs: RealTimeDeque<'T>) (ys: RealTimeDeque<'T>) =
            let front2 = xs.frontLength + xs.rBackLength
            let front3 = xs.frontLength + xs.rBackLength + ys.frontLength
            let back2 = ys.frontLength + ys.rBackLength
            let back3 = xs.rBackLength + ys.frontLength + ys.rBackLength

            match (front2, front3, back2, back3) with
            | a, b, c, d when
                (abs(xs.frontLength - d) <= abs(a - c))
                && (abs(xs.frontLength - d) <= abs(a - ys.rBackLength)
                    && (xs.frontLength > 0))
                ->
                new RealTimeDeque<'T>(
                    cC,
                    xs.frontLength,
                    xs.front,
                    LazyList.empty,
                    (xs.rBackLength + ys.frontLength + ys.rBackLength),
                    (LazyList.append ys.rBack (LazyList.append (LazyList.rev ys.front) xs.rBack)),
                    LazyList.empty
                )
                |> RealTimeDeque.check2
            | a, b, c, d when
                (abs(a - c) <= abs(xs.frontLength - d))
                && (abs(a - c) <= abs(a - ys.rBackLength))
                ->
                new RealTimeDeque<'T>(
                    cC,
                    (xs.frontLength + xs.rBackLength),
                    (LazyList.append xs.front (LazyList.rev xs.rBack)),
                    LazyList.empty,
                    (ys.frontLength + ys.rBackLength),
                    (LazyList.append ys.rBack (LazyList.rev ys.front)),
                    LazyList.empty
                )
                |> RealTimeDeque.check2
            | a, b, c, d ->
                new RealTimeDeque<'T>(
                    cC,
                    (xs.frontLength + xs.rBackLength + ys.frontLength),
                    (LazyList.append (LazyList.append xs.front (LazyList.rev xs.rBack)) ys.front),
                    LazyList.empty,
                    ys.rBackLength,
                    ys.rBack,
                    LazyList.empty
                )
                |> RealTimeDeque.check2

        static member internal Empty c =
            new RealTimeDeque<'T>(c, 0, (LazyList.empty), (LazyList.empty), 0, (LazyList.empty), (LazyList.empty))

        static member internal OfCatListsC c (xs: 'T list) (ys: 'T list) =
            new RealTimeDeque<'T>(c, xs.Length, (LazyList.ofList xs), LazyList.empty, ys.Length, (LazyList.ofList(List.rev ys)), LazyList.empty)
            |> RealTimeDeque.check2

        static member internal OfCatSeqsC c (xs: 'T seq) (ys: 'T seq) =
            new RealTimeDeque<'T>(
                c,
                (Seq.length xs),
                (LazyList.ofSeq xs),
                LazyList.empty,
                (Seq.length ys),
                (LazyList.rev(LazyList.ofSeq ys)),
                LazyList.empty
            )
            |> RealTimeDeque.check2

        static member internal OfSeqC c (xs: seq<'T>) =
            new RealTimeDeque<'T>(c, (Seq.length xs), (LazyList.ofSeq xs), LazyList.empty, 0, LazyList.empty, LazyList.empty)
            |> RealTimeDeque.check2

        ///O(1), worst case. Returns a new deque with the element added to the beginning.
        member this.Cons x =
            new RealTimeDeque<'T>(
                this.c,
                (this.frontLength + 1),
                (LazyList.cons x this.front),
                (RealTimeDeque.exec1 this.streamFront),
                this.rBackLength,
                this.rBack,
                (RealTimeDeque.exec1 this.streamRBack)
            )
            |> RealTimeDeque.check

        ///O(1), worst case. Returns the first element.
        member this.Head =
            match this.front, this.rBack with
            | LazyList.Nil, LazyList.Nil -> raise Exceptions.Empty
            | LazyList.Nil, LazyList.Cons(x, _) -> x
            | LazyList.Cons(x, _), _ -> x

        ///O(1), worst case. Returns option first element.
        member this.TryGetHead =
            match this.front, this.rBack with
            | LazyList.Nil, LazyList.Nil -> None
            | LazyList.Nil, LazyList.Cons(x, _) -> Some(x)
            | LazyList.Cons(x, _), _ -> Some(x)

        ///O(1), worst case. Returns a new deque of the elements before the last element.
        member this.Init =
            match this.front, this.rBack with
            | LazyList.Nil, LazyList.Nil -> raise Exceptions.Empty
            | _, LazyList.Nil -> RealTimeDeque.Empty this.c
            | _, LazyList.Cons(x, xs) ->
                new RealTimeDeque<'T>(
                    this.c,
                    this.frontLength,
                    this.front,
                    (RealTimeDeque.exec2 this.streamFront),
                    (this.rBackLength - 1),
                    xs,
                    (RealTimeDeque.exec2 this.streamRBack)
                )
                |> RealTimeDeque.check

        ///O(1), worst case. Returns option deque of the elements before the last element.
        member this.TryGetInit =
            match this.front, this.rBack with
            | LazyList.Nil, LazyList.Nil -> None
            | _, LazyList.Nil -> Some(RealTimeDeque.Empty this.c)
            | _, LazyList.Cons(x, xs) ->
                Some(
                    new RealTimeDeque<'T>(
                        this.c,
                        this.frontLength,
                        this.front,
                        (RealTimeDeque.exec2 this.streamFront),
                        (this.rBackLength - 1),
                        xs,
                        (RealTimeDeque.exec2 this.streamRBack)
                    )
                    |> RealTimeDeque.check
                )

        ///O(1). Returns true if the deque has no elements.
        member this.IsEmpty = ((this.frontLength = 0) && (this.rBackLength = 0))

        ///O(1), worst case. Returns the last element.
        member this.Last =
            match this.front, this.rBack with
            | LazyList.Nil, LazyList.Nil -> raise Exceptions.Empty
            | _, LazyList.Cons(x, _) -> x
            | LazyList.Cons(x, _), LazyList.Nil -> x

        ///O(1), worst case. Returns option last element.
        member this.TryGetLast =
            match this.front, this.rBack with
            | LazyList.Nil, LazyList.Nil -> None
            | _, LazyList.Cons(x, _) -> Some(x)
            | LazyList.Cons(x, _), LazyList.Nil -> Some(x)

        ///O(1). Returns the count of elements.
        member this.Length = RealTimeDeque.length this

        ///O(n/2), worst case. Returns element by index.
        member this.Lookup(i: int) =
            match frontLength, front, rBackLength, rBack with
            | lenF, front, lenR, rear when i > (lenF + lenR - 1) -> raise Exceptions.OutOfBounds
            | lenF, front, lenR, rear when i < lenF ->
                let rec loopF =
                    function
                    | xs, i' when i' = 0 -> LazyList.head xs
                    | xs, i' -> loopF((LazyList.tail xs), (i' - 1))

                loopF(front, i)
            | lenF, front, lenR, rear ->
                let rec loopF =
                    function
                    | xs, i' when i' = 0 -> LazyList.head xs
                    | xs, i' -> loopF((LazyList.tail xs), (i' - 1))

                loopF(rear, ((lenR - (i - lenF)) - 1))

        ///O(n/2), worst case. Returns option element by index.
        member this.TryLookup(i: int) =
            match frontLength, front, rBackLength, rBack with
            | lenF, front, lenR, rear when i > (lenF + lenR - 1) -> None
            | lenF, front, lenR, rear when i < lenF ->
                let rec loopF =
                    function
                    | xs, i' when i' = 0 -> Some(LazyList.head xs)
                    | xs, i' -> loopF((LazyList.tail xs), (i' - 1))

                loopF(front, i)
            | lenF, front, lenR, rear ->
                let rec loopF =
                    function
                    | xs, i' when i' = 0 -> Some(LazyList.head xs)
                    | xs, i' -> loopF((LazyList.tail xs), (i' - 1))

                loopF(rear, ((lenR - (i - lenF)) - 1))

        ///O(n/2), worst case. Returns deque with element removed by index.
        member this.Remove(i: int) =
            match frontLength, front, rBackLength, rBack with
            | lenF, front, lenR, rear when i > (lenF + lenR - 1) -> raise Exceptions.OutOfBounds
            | lenF, front, lenR, rear when i < lenF ->
                let newFront =
                    if (i = 0) then
                        LazyList.tail front
                    else
                        let left, right = LazyList.split front i
                        LazyList.append (List.rev left |> LazyList.ofList) right

                new RealTimeDeque<'T>(c, (lenF - 1), newFront, LazyList.empty, lenR, rear, LazyList.empty)
                |> RealTimeDeque.check2

            | lenF, front, lenR, rear ->
                let n = lenR - (i - lenF) - 1

                let newRear =
                    if (n = 0) then
                        LazyList.tail rear
                    else
                        let left, right = LazyList.split rear n
                        LazyList.append (List.rev left |> LazyList.ofList) right

                new RealTimeDeque<'T>(c, lenF, front, LazyList.empty, (lenR - 1), newRear, LazyList.empty)
                |> RealTimeDeque.check2

        ///O(n/2), worst case. Returns option deque with element removed by index.
        member this.TryRemove(i: int) =
            match frontLength, front, rBackLength, rBack with
            | lenF, front, lenR, rear when i > (lenF + lenR - 1) -> None
            | lenF, front, lenR, rear when i < lenF ->
                let newFront =
                    if (i = 0) then
                        LazyList.tail front
                    else
                        let left, right = LazyList.split front i
                        LazyList.append (List.rev left |> LazyList.ofList) right

                let z =
                    new RealTimeDeque<'T>(c, (lenF - 1), newFront, LazyList.empty, lenR, rear, LazyList.empty)
                    |> RealTimeDeque.check2

                Some(z)

            | lenF, front, lenR, rear ->
                let n = lenR - (i - lenF) - 1

                let newRear =
                    if (n = 0) then
                        LazyList.tail rear
                    else
                        let left, right = LazyList.split rear n
                        LazyList.append (List.rev left |> LazyList.ofList) right

                let z =
                    new RealTimeDeque<'T>(c, lenF, front, LazyList.empty, (lenR - 1), newRear, LazyList.empty)
                    |> RealTimeDeque.check2

                Some(z)

        ///O(1). Returns deque reversed.
        member this.Rev =
            (new RealTimeDeque<'T>(c, rBackLength, rBack, streamRBack, frontLength, front, streamFront))

        ///O(1), worst case. Returns a new deque with the element added to the end.
        member this.Snoc x =
            new RealTimeDeque<'T>(
                this.c,
                this.frontLength,
                this.front,
                (RealTimeDeque.exec1 this.streamFront),
                (this.rBackLength + 1),
                (LazyList.cons x this.rBack),
                (RealTimeDeque.exec1 this.streamRBack)
            )
            |> RealTimeDeque.check

        ///O(1), worst case. Returns a new deque of the elements trailing the first element.
        member this.Tail =
            match this.front, this.rBack with
            | LazyList.Nil, LazyList.Nil -> raise Exceptions.Empty
            | LazyList.Nil, LazyList.Cons(x, _) -> RealTimeDeque.Empty this.c
            | LazyList.Cons(x, xs), _ ->
                new RealTimeDeque<'T>(
                    this.c,
                    (this.frontLength - 1),
                    xs,
                    (RealTimeDeque.exec2 this.streamFront),
                    this.rBackLength,
                    this.rBack,
                    (RealTimeDeque.exec2 this.streamRBack)
                )
                |> RealTimeDeque.check

        ///O(1), worst case. Returns option deque of the elements trailing the first element.
        member this.TryGetTail =
            match this.front, this.rBack with
            | LazyList.Nil, LazyList.Nil -> None
            | LazyList.Nil, LazyList.Cons(x, _) -> Some(RealTimeDeque.Empty this.c)
            | LazyList.Cons(x, xs), _ ->
                Some(
                    new RealTimeDeque<'T>(
                        this.c,
                        (this.frontLength - 1),
                        xs,
                        (RealTimeDeque.exec2 this.streamFront),
                        this.rBackLength,
                        this.rBack,
                        (RealTimeDeque.exec2 this.streamRBack)
                    )
                    |> RealTimeDeque.check
                )

        ///O(1), worst case. Returns the first element and tail
        member this.Uncons =
            match this.front, this.rBack with
            | LazyList.Nil, LazyList.Nil -> raise Exceptions.Empty
            | _, _ -> this.Head, this.Tail

        ///O(1), worst case. Returns option first element and tail.
        member this.TryUncons =
            match this.front, this.rBack with
            | LazyList.Nil, LazyList.Nil -> None
            | _, _ -> Some(this.Head, this.Tail)

        ///O(1), worst case. Returns init and the last element.
        member this.Unsnoc =
            match this.front, this.rBack with
            | LazyList.Nil, LazyList.Nil -> raise Exceptions.Empty
            | _, _ -> this.Init, this.Last

        ///O(1), worst case. Returns option init and the last element.
        member this.TryUnsnoc =
            match this.front, this.rBack with
            | LazyList.Nil, LazyList.Nil -> None
            | _, _ -> Some(this.Init, this.Last)

        ///O(n/2), worst case. Returns deque with element updated by index.
        member this.Update (i: int) (y: 'T) =
            match frontLength, front, rBackLength, rBack with
            | lenF, front, lenR, rear when i > (lenF + lenR - 1) -> raise Exceptions.OutOfBounds
            | lenF, front, lenR, rear when i < lenF ->
                let newFront =
                    if (i = 0) then
                        LazyList.cons y (LazyList.tail front)
                    else
                        let left, right = LazyList.split front i
                        LazyList.append (List.rev left |> LazyList.ofList) (LazyList.cons y right)

                new RealTimeDeque<'T>(c, lenF, newFront, LazyList.empty, lenR, rear, LazyList.empty)
                |> RealTimeDeque.check2

            | lenF, front, lenR, rear ->
                let n = lenR - (i - lenF) - 1

                let newRear =
                    if (n = 0) then
                        LazyList.cons y (LazyList.tail rear)
                    else
                        let left, right = LazyList.split rear n
                        LazyList.append (List.rev left |> LazyList.ofList) (LazyList.cons y right)

                new RealTimeDeque<'T>(c, lenF, front, LazyList.empty, lenR, newRear, LazyList.empty)
                |> RealTimeDeque.check2

        ///O(n/2), worst case. Returns option deque with element updated by index.
        member this.TryUpdate (i: int) (y: 'T) =
            match frontLength, front, rBackLength, rBack with
            | lenF, front, lenR, rear when i > (lenF + lenR - 1) -> None
            | lenF, front, lenR, rear when i < lenF ->
                let newFront =
                    if (i = 0) then
                        LazyList.cons y (LazyList.tail front)
                    else
                        let left, right = LazyList.split front i
                        LazyList.append (List.rev left |> LazyList.ofList) (LazyList.cons y right)

                let z =
                    new RealTimeDeque<'T>(c, lenF, newFront, LazyList.empty, lenR, rear, LazyList.empty)
                    |> RealTimeDeque.check2

                Some(z)

            | lenF, front, lenR, rear ->
                let n = lenR - (i - lenF) - 1

                let newRear =
                    if (n = 0) then
                        LazyList.cons y (LazyList.tail rear)
                    else
                        let left, right = LazyList.split rear n
                        LazyList.append (List.rev left |> LazyList.ofList) (LazyList.cons y right)

                let z =
                    new RealTimeDeque<'T>(c, lenF, front, LazyList.empty, lenR, newRear, LazyList.empty)
                    |> RealTimeDeque.check2

                Some(z)

        interface IReadOnlyList<'T> with
            member this.Count = this.Length

            member this.Item
                with get i = this.Lookup i

            member this.GetEnumerator() =
                let e = seq {
                    yield! front
                    yield! (LazyList.rev this.rBack)
                }

                e.GetEnumerator()

            member this.GetEnumerator() =
                (this :> _ seq).GetEnumerator() :> IEnumerator

    [<RequireQualifiedAccess>]
    module Deque =
        //pattern discriminators

        let (|Cons|Nil|)(q: RealTimeDeque<'T>) =
            match q.TryUncons with
            | Some(a, b) -> Cons(a, b)
            | None -> Nil

        let (|Snoc|Nil|)(q: RealTimeDeque<'T>) =
            match q.TryUnsnoc with
            | Some(a, b) -> Snoc(a, b)
            | None -> Nil

        let private stndC = 2

        ///O(|ys-xs|). Returns a deque of the two deques concatenated, front-back stream ratio constant defaulted to 2.
        let append (xs: RealTimeDeque<'T>) (ys: RealTimeDeque<'T>) =
            RealTimeDeque.AppendC stndC xs ys

        ///O(|ys-xs|). Returns a deque of the two deques concatenated, c is front-back stream ratio constant, should be at least 2.
        let appendC c (xs: RealTimeDeque<'T>) (ys: RealTimeDeque<'T>) =
            RealTimeDeque.AppendC c xs ys

        ///O(1), worst case. Returns a new deque with the element added to the beginning.
        let inline cons (x: 'T) (q: RealTimeDeque<'T>) = q.Cons x

        ///O(1). Returns deque of no elements, c is front-back stream ration constant, should be at least 2.
        let empty c =
            RealTimeDeque.Empty c

        ///O(1), worst case. Returns the first element.
        let inline head(q: RealTimeDeque<'T>) = q.Head

        ///O(1), worst case. Returns option first element.
        let inline tryGetHead(q: RealTimeDeque<'T>) =
            q.TryGetHead

        ///O(1), worst case. Returns a new deque of the elements before the last element.
        let inline init(q: RealTimeDeque<'T>) = q.Init

        ///O(1), worst case. Returns option deque of the elements before the last element.
        let inline tryGetInit(q: RealTimeDeque<'T>) =
            q.TryGetInit

        ///O(1). Returns true if the deque has no elements.
        let inline isEmpty(q: RealTimeDeque<'T>) = q.IsEmpty

        ///O(1), worst case. Returns the last element.
        let inline last(q: RealTimeDeque<'T>) = q.Last

        ///O(1), worst case. Returns option last element.
        let inline tryGetLast(q: RealTimeDeque<'T>) =
            q.TryGetLast

        ///O(1). Returns the count of elements.
        let inline length(q: RealTimeDeque<'T>) = q.Length

        ///O(n/2), worst case. Returns option element by index.
        let inline lookup i (q: RealTimeDeque<'T>) =
            q.Lookup i

        ///O(n/2), worst case. Returns option element by index.
        let inline tryLookup i (q: RealTimeDeque<'T>) =
            q.TryLookup i

        ///O(|ys-xs|). Returns a deque of the two lists concatenated, front-back stream ratio constant defaulted to 2.
        let ofCatLists xs ys =
            RealTimeDeque.OfCatListsC stndC xs ys

        ///O(|ys-xs|). Returns a deque of the two lists concatenated, c is front-back stream ration constant, should be at least 2.
        let ofCatListsC c xs ys =
            RealTimeDeque.OfCatListsC c xs ys

        ///O(|ys-xs|). Returns a deque of the two seqs concatenated, front-back stream ratio constant defaulted to 2.
        let ofCatSeqs xs ys =
            RealTimeDeque.OfCatSeqsC stndC xs ys

        ///O(|ys-xs|). Returns a deque of the two seqs concatenated, c is front-back stream ratio constant, should be at least 2.
        let ofCatSeqsC c xs ys =
            RealTimeDeque.OfCatSeqsC c xs ys

        ///O(n). Returns a deque of the seq, front-back stream ratio constant defaulted to 2.
        let ofSeq xs =
            RealTimeDeque.OfSeqC stndC xs

        ///O(n). Returns a deque of the seq, c is front-back stream ratio constant, should be at least 2.
        let ofSeqC c xs =
            RealTimeDeque.OfSeqC c xs

        ///O(n/2), worst case. Returns deque with element removed by index.
        let inline remove i (q: RealTimeDeque<'T>) =
            q.Remove i

        ///O(n/2), worst case. Returns option deque with element removed by index.
        let inline tryRemove i (q: RealTimeDeque<'T>) =
            q.TryRemove i

        ///O(1). Returns deque reversed.
        let inline rev(q: RealTimeDeque<'T>) = q.Rev

        ///O(1). Returns a deque of one element, front-back stream ratio constant defaulted to 2.
        let singleton x =
            empty stndC |> cons x

        ///O(1). Returns a deque of one element, c is front-back stream ratio constant, should be at least 2.
        let singletonC c x =
            empty c |> cons x

        ///O(1), worst case. Returns a new deque with the element added to the end.
        let inline snoc (x: 'T) (q: RealTimeDeque<'T>) =
            (q.Snoc x)

        ///O(1), worst case. Returns a new deque of the elements trailing the first element.
        let inline tail(q: RealTimeDeque<'T>) = q.Tail

        ///O(1), worst case. Returns option deque of the elements trailing the first element.
        let inline tryGetTail(q: RealTimeDeque<'T>) =
            q.TryGetTail

        ///O(1), worst case. Returns the first element and tail.
        let inline uncons(q: RealTimeDeque<'T>) = q.Uncons

        ///O(1), worst case. Returns option first element and tail.
        let inline tryUncons(q: RealTimeDeque<'T>) =
            q.TryUncons

        ///O(1), worst case. Returns init and the last element.
        let inline unsnoc(q: RealTimeDeque<'T>) = q.Unsnoc

        ///O(1), worst case. Returns option init and the last element.
        let inline tryUnsnoc(q: RealTimeDeque<'T>) =
            q.TryUnsnoc

        ///O(n/2), worst case. Returns deque with element updated by index.
        let inline update i y (q: RealTimeDeque<'T>) =
            q.Update i y

        ///O(n/2), worst case. Returns option deque with element updated by index.
        let inline tryUpdate i y (q: RealTimeDeque<'T>) =
            q.TryUpdate i y