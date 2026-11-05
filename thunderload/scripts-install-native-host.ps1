param(
  [Parameter(Mandatory=$true)]
  [string]$ExtensionId
)

$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$HostExe = Join-Path $ProjectRoot "target\release\thunderload-host.exe"
if (!(Test-Path $HostExe)) {
  throw "Native host executable not found. Build it first: cargo build --release --manifest-path native-host/Cargo.toml"
}

$InstallDir = Join-Path $env:LOCALAPPDATA "ThunderLoad"
New-Item -ItemType Directory -Force -Path $InstallDir | Out-Null
$ManifestPath = Join-Path $InstallDir "native-host.json"
$Manifest = @{
  name = "com.thunderload.host"
  description = "ThunderLoad local download engine"
  path = $HostExe
  type = "stdio"
  allowed_origins = @("chrome-extension://$ExtensionId/")
  allowed_extensions = @("thunderload@example.com")
}
$ManifestJson = $Manifest | ConvertTo-Json -Depth 5
[System.IO.File]::WriteAllText($ManifestPath, $ManifestJson, [System.Text.UTF8Encoding]::new($false))

$RegistryPaths = @(
  "HKCU:\Software\Google\Chrome\NativeMessagingHosts\com.thunderload.host",
  "HKCU:\Software\Microsoft\Edge\NativeMessagingHosts\com.thunderload.host",
  "HKCU:\Software\BraveSoftware\Brave-Browser\NativeMessagingHosts\com.thunderload.host",
  "HKCU:\Software\Mozilla\NativeMessagingHosts\com.thunderload.host"
)
foreach ($KeyPath in $RegistryPaths) {
  New-Item -Path $KeyPath -Force | Out-Null
  Set-Item -Path $KeyPath -Value $ManifestPath
}
Write-Host "ThunderLoad native host registered for Chrome, Edge, Brave and Firefox."
Write-Host "Restart your browser after installing/reloading the extension."
Write-Host "Host manifest: $ManifestPath"
