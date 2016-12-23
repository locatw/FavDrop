module FavDrop.ExponentialBackoff

[<Measure>]
type millisecond

type RetryActionResult<'a> =
| Success of 'a
| Retry

type RetryConfig = {
    WaitTime : int<millisecond>
    MaxWaitTime : int<millisecond>
}

type RetryAsync<'a> = RetryConfig -> (unit -> Async<RetryActionResult<'a>>) -> Async<unit>

let private nextRetryConfig currentConfig =
    let waitTime = 2 * currentConfig.WaitTime
    let newWaitTime =
        if waitTime <= currentConfig.MaxWaitTime then
            waitTime
        else
            currentConfig.MaxWaitTime

    { currentConfig with WaitTime = newWaitTime }

let internal retryAsyncInternal (sleep : int<millisecond> -> Async<unit>) retryConfig (f : unit -> Async<RetryActionResult<'a>>) =
    async {
        let rec loop f retryConfig =
            async {
                let! result = f()
                match result with
                | Success x -> return x
                | Retry ->
                    do! sleep retryConfig.WaitTime
                    let newRetryConfig = nextRetryConfig retryConfig
                    return! loop f newRetryConfig
            }

        return! loop f retryConfig
    }

let retryAsync retryConfig (f : unit -> Async<RetryActionResult<'a>>) =
    async {
        return! retryAsyncInternal (fun x -> x |> (int >> Async.Sleep)) retryConfig f
    }
