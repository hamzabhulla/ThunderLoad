param(
  [string]$ChromeExtensionId,
  [string]$EdgeExtensionId,
  [string]$BraveExtensionId,
  [string]$OperaExtensionId,
  [string]$VivaldiExtensionId,
  [string]$FirefoxExtensionId = "thunderload@example.com"
)
$ErrorActionPreference = "Stop"
$InstallDir = Join-Path $env:LOCALAPPDATA "ThunderLoad\native-host"
$HostSource = Join-Path $PSScriptRoot "thunderload-host.exe"
if (!(Test-Path $HostSource)) { throw "thunderload-host.exe was not found beside this script. Use the native-host setup package." }
New-Item -ItemType Directory -Force -Path $InstallDir | Out-Null
$HostExe = Join-Path $InstallDir "thunderload-host.exe"
Copy-Item -LiteralPath $HostSource -Destination $HostExe -Force
function Write-Manifest($Manifest, [string]$Path) {
  $Json = $Manifest | ConvertTo-Json -Depth 5
  [System.IO.File]::WriteAllText($Path, $Json, [System.Text.UTF8Encoding]::new($false))
}
function Register-ChromiumHost([string]$Browser, [string]$ExtensionId, [string]$RegistryPath) {
  if ([string]::IsNullOrWhiteSpace($ExtensionId)) { return }
  if ($ExtensionId -notmatch '^[a-p]{32}$') { throw "$Browser ID must be the exact 32-character store/unpacked extension ID." }
  $ManifestPath = Join-Path $InstallDir "$Browser-native-host.json"
  Write-Manifest @{ name="com.thunderload.host"; description="ThunderLoad local download engine"; path=$HostExe; type="stdio"; allowed_origins=@("chrome-extension://$ExtensionId/") } $ManifestPath
  New-Item -Path $RegistryPath -Force | Out-Null
  Set-Item -Path $RegistryPath -Value $ManifestPath
  Write-Host "$Browser native host registered."
}
function Register-FirefoxHost([string]$ExtensionId) {
  if ([string]::IsNullOrWhiteSpace($ExtensionId)) { return }
  $ManifestPath = Join-Path $InstallDir "firefox-native-host.json"
  Write-Manifest @{ name="com.thunderload.host"; description="ThunderLoad local download engine"; path=$HostExe; type="stdio"; allowed_extensions=@($ExtensionId) } $ManifestPath
  $RegistryPath = "HKCU:\Software\Mozilla\NativeMessagingHosts\com.thunderload.host"
  New-Item -Path $RegistryPath -Force | Out-Null
  Set-Item -Path $RegistryPath -Value $ManifestPath
  Write-Host "Firefox native host registered."
}
Register-ChromiumHost "chrome" $ChromeExtensionId "HKCU:\Software\Google\Chrome\NativeMessagingHosts\com.thunderload.host"
Register-ChromiumHost "edge" $EdgeExtensionId "HKCU:\Software\Microsoft\Edge\NativeMessagingHosts\com.thunderload.host"
Register-ChromiumHost "brave" $BraveExtensionId "HKCU:\Software\BraveSoftware\Brave-Browser\NativeMessagingHosts\com.thunderload.host"
Register-ChromiumHost "opera" $OperaExtensionId "HKCU:\Software\Opera Software\NativeMessagingHosts\com.thunderload.host"
Register-ChromiumHost "vivaldi" $VivaldiExtensionId "HKCU:\Software\Vivaldi\NativeMessagingHosts\com.thunderload.host"
Register-FirefoxHost $FirefoxExtensionId
Write-Host "Done. Restart the selected browsers."
