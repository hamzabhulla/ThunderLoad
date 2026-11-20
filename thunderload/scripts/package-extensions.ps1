#Requires -Version 7.0
# PowerShell 7+ only. Windows PowerShell 5.1's Compress-Archive writes
# backslash-separated ZIP entry names, which Firefox/Chrome treat as
# literal filenames instead of paths — so this script never calls
# Compress-Archive at all: it writes the archives with .NET's ZipFile
# API and forward-slash entry names (see New-ForwardSlashZip), and
# verifies the written archives contain no backslash entries.
$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
$Extension = Join-Path $Root "extension"
$Output = Join-Path $Root "dist" "extensions"
$Stage = Join-Path ([System.IO.Path]::GetTempPath()) ("thunderload-" + [guid]::NewGuid().ToString("N"))

# Explicit allowlist: the ONLY files that ship in either package.
# A denylist (copy the whole dir, remove a few names) silently ships
# anything new that lands in extension/ — a mockup, a scratch file,
# a stray manifest — so both packagers build from this list and
# nothing else. Keep in sync with scripts/package-extensions.mjs
# (EXTENSION_FILES). The two manifests are handled per-browser below:
# Chromium ships manifest.json, Firefox ships manifest.firefox.json
# renamed to manifest.json.
$CommonFiles = @(
  "background.js",
  "content.js",
  "hook.js",
  "popup.css",
  "popup.html",
  "popup.js",
  "icons/16.png",
  "icons/32.png",
  "icons/48.png",
  "icons/128.png"
)

function Copy-Allowlisted {
  param([string]$Destination, [string]$ManifestFile, [string]$ManifestArchiveName)
  New-Item -ItemType Directory -Force -Path $Destination | Out-Null
  foreach ($relative in $CommonFiles) {
    $source = Join-Path $Extension ($relative -replace "/", [System.IO.Path]::DirectorySeparatorChar)
    if (-not (Test-Path $source -PathType Leaf)) { throw "Allowlisted extension file is missing: $relative" }
    $target = Join-Path $Destination ($relative -replace "/", [System.IO.Path]::DirectorySeparatorChar)
    New-Item -ItemType Directory -Force -Path (Split-Path -Parent $target) | Out-Null
    Copy-Item $source $target -Force
  }
  $manifestSource = Join-Path $Extension $ManifestFile
  if (-not (Test-Path $manifestSource -PathType Leaf)) { throw "Allowlisted extension file is missing: $ManifestFile" }
  Copy-Item $manifestSource (Join-Path $Destination $ManifestArchiveName) -Force
}

function New-ForwardSlashZip {
  param([string]$SourceDir, [string]$DestinationPath)
  if (Test-Path $DestinationPath) { Remove-Item $DestinationPath -Force }
  $zip = [System.IO.Compression.ZipFile]::Open($DestinationPath, [System.IO.Compression.ZipArchiveMode]::Create)
  try {
    $files = Get-ChildItem -Path $SourceDir -Recurse -File | Sort-Object FullName
    foreach ($file in $files) {
      # Forward slashes only: GetRelativePath returns the OS
      # separator (backslash on Windows), which must never reach
      # the archive — ZIP entry names are slash-separated.
      $relative = [System.IO.Path]::GetRelativePath($SourceDir, $file.FullName).Replace([System.IO.Path]::DirectorySeparatorChar, "/")
      [System.IO.Compression.ZipFileExtensions]::CreateEntryFromFile($zip, $file.FullName, $relative, [System.IO.Compression.CompressionLevel]::Optimal) | Out-Null
    }
  } finally { $zip.Dispose() }
  # Verify what was written: no entry may contain a backslash.
  $check = [System.IO.Compression.ZipFile]::OpenRead($DestinationPath)
  try {
    foreach ($entry in $check.Entries) {
      if ($entry.FullName.Contains("\")) { throw "Archive contains a backslash entry name: $($entry.FullName)" }
    }
  } finally { $check.Dispose() }
}

New-Item -ItemType Directory -Force -Path $Output,$Stage | Out-Null
try {
  $Chrome = Join-Path $Stage "chromium"
  Copy-Allowlisted -Destination $Chrome -ManifestFile "manifest.json" -ManifestArchiveName "manifest.json"
  New-ForwardSlashZip -SourceDir $Chrome -DestinationPath (Join-Path $Output "ThunderLoad-Chromium.zip")
  $Firefox = Join-Path $Stage "firefox"
  Copy-Allowlisted -Destination $Firefox -ManifestFile "manifest.firefox.json" -ManifestArchiveName "manifest.json"
  New-ForwardSlashZip -SourceDir $Firefox -DestinationPath (Join-Path $Output "ThunderLoad-Firefox.zip")
  # An .xpi is a ZIP with a different extension; the desktop app serves the
  # .xpi for Firefox (see save_extension_file). The Tauri bundler lists the
  # .xpi as a resource, so it must exist before the installer build runs.
  Copy-Item (Join-Path $Output "ThunderLoad-Firefox.zip") (Join-Path $Output "ThunderLoad-Firefox.xpi") -Force
  Write-Host "Extension packages written to $Output"
} finally { Remove-Item $Stage -Recurse -Force -ErrorAction SilentlyContinue }
