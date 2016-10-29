#r @"packages/FAKE/tools/FakeLib.dll"

open Fake

let projectRootDir = "FavDrop"
let releaseDir = projectRootDir @@ "bin/Release"
let packDir = "pack/"
let packedAppRootDir = packDir @@ "FavDrop"

Target "BuildApp" (fun _ ->
    !! (projectRootDir @@ "/**/*.fsproj")
    |> MSBuildRelease releaseDir "Build"
    |> Log "AppBuild-Output: "
)

Target "Pack" (fun _ ->
    if not (TestDir packedAppRootDir) then
        CreateDir packedAppRootDir
    else
        CleanDir packedAppRootDir

    !! (releaseDir @@ "*.exe")
    ++ (releaseDir @@ "*.dll")
    ++ (releaseDir @@ "*.config")
    -- (releaseDir @@ "*.secret.config")
    |> Copy packedAppRootDir
)

Target "MakeReleaseZip" (fun _ ->
    !! (packDir @@ "**/*.*")
    |> Zip packDir "FavDrop.zip"
)

"BuildApp"
    ==> "Pack"
    ==> "MakeReleaseZip"

RunTargetOrDefault "BuildApp"