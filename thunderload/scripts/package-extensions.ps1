$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
$Extension = Join-Path $Root "extension"
$Output = Join-Path $Root "dist\extensions"
$Stage = Join-Path $env:TEMP ("thunderload-" + [guid]::NewGuid().ToString("N"))
New-Item -ItemType Directory -Force -Path $Output,$Stage | Out-Null
try {
  $Chrome = Join-Path $Stage "chromium"
  Copy-Item $Extension $Chrome -Recurse
  Remove-Item (Join-Path $Chrome "manifest.firefox.json") -Force
  Compress-Archive -Path (Join-Path $Chrome "*") -DestinationPath (Join-Path $Output "ThunderLoad-Chromium.zip") -Force
  $Firefox = Join-Path $Stage "firefox"
  Copy-Item $Extension $Firefox -Recurse
  Remove-Item (Join-Path $Firefox "manifest.json") -Force
  Move-Item (Join-Path $Firefox "manifest.firefox.json") (Join-Path $Firefox "manifest.json")
  Compress-Archive -Path (Join-Path $Firefox "*") -DestinationPath (Join-Path $Output "ThunderLoad-Firefox.zip") -Force
  Write-Host "Extension packages written to $Output"
} finally { Remove-Item $Stage -Recurse -Force -ErrorAction SilentlyContinue }
