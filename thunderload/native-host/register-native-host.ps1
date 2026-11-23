param(
  [string]$ChromeExtensionId,
  [string]$EdgeExtensionId,
  [string]$BraveExtensionId,
  [string]$OperaExtensionId,
  [string]$VivaldiExtensionId,
  # No default: Firefox is registered ONLY when an ID is passed in.
  # The old default ("thunderload@example.com") registered Firefox on
  # every run even when the user only asked for a Chromium browser.
  # The desktop app fills in thunderload@example.com itself when the
  # user picks Firefox there, so nothing is lost by requiring it here.
  [string]$FirefoxExtensionId = ""
)
$ErrorActionPreference = "Stop"
$InstallDir = Join-Path $env:LOCALAPPDATA "ThunderLoad\native-host"
$HostSource = Join-Path $PSScriptRoot "thunderload-host.exe"
if (!(Test-Path $HostSource)) { throw "thunderload-host.exe was not found beside this script. Use the native-host setup package." }
New-Item -ItemType Directory -Force -Path $InstallDir | Out-Null
# Content-addressed install (same reason as the desktop app): Windows locks
# a running executable's file, so copying over a fixed thunderload-host.exe
# fails with a sharing violation while Chrome has the host running. The
# installed copy is named after its content hash; identical bytes already
# installed are never rewritten, and a new binary never overwrites the file
# a running host executes from. The manifests below point at $HostExe, so
# they follow the versioned file automatically. Stale installs are pruned
# at the end of this script (see below): every manifest written here
# points at the current $HostExe, so an older versioned file is referenced
# by nothing — except a host process still executing from it, which holds
# a Windows file lock; locked files are left alone and pruned on a later
# run once the browser has closed.
$SourceHash = (Get-FileHash -LiteralPath $HostSource -Algorithm SHA256).Hash
$HostExe = Join-Path $InstallDir ("thunderload-host-" + $SourceHash.Substring(0, 16).ToLowerInvariant() + ".exe")
$InstalledHash = if (Test-Path -LiteralPath $HostExe) { (Get-FileHash -LiteralPath $HostExe -Algorithm SHA256).Hash } else { $null }
if ($InstalledHash -ne $SourceHash) {
  Copy-Item -LiteralPath $HostSource -Destination $HostExe -Force
}
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
  # Same rule as the desktop's Firefox validation: an add-on ID is an
  # email-like ID of at most 128 characters (letters, digits and
  # @ . _ + -; the fixed ThunderLoad ID is thunderload@example.com)
  # or a braced GUID {xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx}. A
  # Chromium-style 32-character a-p ID pasted here must fail loudly,
  # not register a manifest Firefox will never match. -cmatch (not
  # -match) keeps the rejection case-sensitive, exactly like the
  # desktop's lowercase a-p check.
  if ($ExtensionId -cmatch '^[a-p]{32}$') { throw "Firefox ID must be a valid add-on ID (for example thunderload@example.com). A 32-character a-p ID is a Chromium extension ID, not a Firefox add-on ID." }
  if ($ExtensionId.Length -gt 128 -or ($ExtensionId -notmatch '^[A-Za-z0-9@._+-]+$' -and $ExtensionId -notmatch '^\{[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}\}$')) { throw "Firefox ID must be a valid add-on ID (for example thunderload@example.com)." }
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
# Opera is Chromium-based and reads Chrome's native-messaging key —
# there is no working "Opera Software" key, so registering Opera under
# its own key wrote a value no Opera build ever reads. Opera shares
# Chrome's key (and therefore Chrome's manifest slot: whichever of
# Chrome/Opera registers last wins the key).
Register-ChromiumHost "opera" $OperaExtensionId "HKCU:\Software\Google\Chrome\NativeMessagingHosts\com.thunderload.host"
Register-ChromiumHost "vivaldi" $VivaldiExtensionId "HKCU:\Software\Vivaldi\NativeMessagingHosts\com.thunderload.host"
if (-not [string]::IsNullOrWhiteSpace($FirefoxExtensionId)) { Register-FirefoxHost $FirefoxExtensionId }
# Clean up a stale registration an older installer may have written
# under the dead Opera key (see above): it points at an old manifest
# no Opera build reads.
$LegacyOperaKey = "HKCU:\Software\Opera Software\NativeMessagingHosts\com.thunderload.host"
if (Test-Path $LegacyOperaKey) { Remove-Item -Path $LegacyOperaKey -Recurse -Force }
# Prune versioned host binaries older than the one just installed.
# A file a running host executes from is locked and cannot be deleted;
# that failure is expected and ignored — the file is pruned on a later
# run once the browser has closed and released it.
Get-ChildItem -LiteralPath $InstallDir -Filter "thunderload-host-*.exe" -File -ErrorAction SilentlyContinue | Where-Object { $_.FullName -ne $HostExe } | ForEach-Object {
  try { Remove-Item -LiteralPath $_.FullName -Force -ErrorAction Stop } catch { }
}
Write-Host "Done. Restart the selected browsers."
