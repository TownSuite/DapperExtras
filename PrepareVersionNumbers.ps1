#!/usr/bin/env pwsh
$ErrorActionPreference = "Stop"
$CURRENTPATH=$pwd.Path

function RunVerionUpdater($loc, $path){
	If ($IsWindows) {
	    Write-Output "Running on Windows, not supported"
	}
	else {
		assemblyinfoutil -inc:$loc "$path"
	}	
}

RunVerionUpdater 2 "$CURRENTPATH/Directory.Build.props"
