module FavDrop.ExponentialBackoff

[<Measure>]
type millisecond

type RetryActionResult =
| Retry
| NoRetry

type RetryConfig = {
    WaitTime : int<millisecond>
    MaxWaitTime : int<millisecond>
}

type RetryAsync = RetryConfig -> (unit -> RetryActionResult) -> Async<unit>

let private nextRetryConfig currentConfig =
    let waitTime = 2 * currentConfig.WaitTime
    let newWaitTime =
        if waitTime <= currentConfig.MaxWaitTime then
            waitTime
        else
            currentConfig.MaxWaitTime

    { currentConfig with WaitTime = newWaitTime }

let internal retryAsyncInternal (sleep : int<millisecond> -> Async<unit>) retryConfig (f : unit -> RetryActionResult) = async {
    let rec loop f retryConfig = async {
        let result = f()
        match result with
        | Retry ->
            do! sleep retryConfig.WaitTime
            let newRetryConfig = nextRetryConfig retryConfig
            return! loop f newRetryConfig
        | NoRetry ->
            ()
    }

    return! loop f retryConfig
}

let retryAsync retryConfig (f : unit -> RetryActionResult) = async {
    return! retryAsyncInternal (fun x -> x |> (int >> Async.Sleep)) retryConfig f
}
