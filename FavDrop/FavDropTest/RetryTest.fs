namespace FavDropTest

open NUnit.Framework
open Microsoft.FSharp.Core.LanguagePrimitives
open FavDrop.ExponentialBackoff
open FsUnit

[<TestFixture>]
module RetryTest =
    let private asyncSleepStub x = async { return () }

    let private noWaitRetryAsync = retryAsyncInternal asyncSleepStub

    [<Test>]
    let ``not retry when returned NoRetry`` () =
        let mutable calledCount = 0

        let f () = async {
            calledCount <- calledCount + 1
            return NoRetry
        }
        let initialRetryConfig =
            { WaitTime = Int32WithMeasure(1); MaxWaitTime = Int32WithMeasure(2) }

        async { return! noWaitRetryAsync initialRetryConfig f }
        |> Async.RunSynchronously

        calledCount |> should equal 1

    [<Test>]
    let ``retry repeatedly until NoRety returned`` () =
        let expectedCalledCount = 3

        let mutable calledCount = 0
        let f () = async {
            calledCount <- calledCount + 1
            if calledCount < expectedCalledCount then
                return Retry
            else
                return NoRetry
        }
        let initialRetryConfig =
            { WaitTime = Int32WithMeasure(1); MaxWaitTime = Int32WithMeasure(2) }

        async { return! noWaitRetryAsync initialRetryConfig f }
        |> Async.RunSynchronously

        calledCount |> should equal expectedCalledCount

    [<Test>]
    let ``sleep time increases exponentially`` () =
        let funWithMaxTimes maxTimes =
            let mutable calledCount = 0
            fun () ->
                let f () = async {
                    calledCount <- calledCount + 1
                    if calledCount <= maxTimes then
                        return Retry
                    else
                        return NoRetry
                }
                f()

        let mutable actualWaitTimes = []
        let sleep (time : int<millisecond>) = async {
            actualWaitTimes <- (int time) :: actualWaitTimes
        }

        let initialRetryConfig =
            { WaitTime = Int32WithMeasure(1); MaxWaitTime = Int32WithMeasure(100) }
        async { return! retryAsyncInternal sleep initialRetryConfig (funWithMaxTimes 4) }
        |> Async.RunSynchronously

        actualWaitTimes
        |> List.rev
        |> should equal [1; 2; 4; 8]

    [<Test>]
    let ``sleep time increases exponentially but does not over max wait time`` () =
        let funWithMaxTimes maxTimes =
            let mutable calledCount = 0
            fun () ->
                let f () = async {
                    calledCount <- calledCount + 1
                    if calledCount <= maxTimes then
                        return Retry
                    else
                        return NoRetry
                    }
                f()

        let maxWaitTime = 10

        let mutable actualWaitTimes = []
        let sleep (time : int<millisecond>) = async {
            actualWaitTimes <- (int time) :: actualWaitTimes
        }

        let initialRetryConfig =
            { WaitTime = Int32WithMeasure(1); MaxWaitTime = Int32WithMeasure(maxWaitTime) }
        async { return! retryAsyncInternal sleep initialRetryConfig (funWithMaxTimes 10) }
        |> Async.RunSynchronously

        actualWaitTimes
        |> List.max
        |> should equal maxWaitTime
