#r @"packages/FAKE/tools/FakeLib.dll"

open Fake

let projectRootDir = "FavDrop"
let releaseDir = projectRootDir @@ "bin/Release"
let packDir = "pack/"

Target "BuildApp" (fun _ ->
    !! (projectRootDir @@ "/**/*.fsproj")
    |> MSBuildRelease releaseDir "Build"
    |> Log "AppBuild-Output: "
)

Target "Pack" (fun _ ->
    if not (TestDir packDir) then
        CreateDir packDir
    else
        CleanDir packDir

    !! (releaseDir @@ "*.exe")
    ++ (releaseDir @@ "*.dll")
    ++ (releaseDir @@ "*.config")
    -- (releaseDir @@ "*.secret.config")
    |> Copy packDir
)

Target "MakeReleaseZip" (fun _ ->
    !! (packDir @@ "**/*.*")
    |> Zip packDir "FavDrop.zip"
)

"BuildApp"
    ==> "Pack"
    ==> "MakeReleaseZip"

RunTargetOrDefault "BuildApp"