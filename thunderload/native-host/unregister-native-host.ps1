# Removes every ThunderLoad browser native-messaging registration and
# the installed host binaries. Fails LOUDLY: any removal that errors
# aborts the script with a non-zero exit instead of printing success
# anyway (the old script swallowed every error and always claimed
# success). A key that is already gone is not a failure — only a real
# removal error is.
$ErrorActionPreference = "Stop"
$Failures = @()
$Keys = @(
  "HKCU:\Software\Google\Chrome\NativeMessagingHosts\com.thunderload.host",
  "HKCU:\Software\Microsoft\Edge\NativeMessagingHosts\com.thunderload.host",
  "HKCU:\Software\BraveSoftware\Brave-Browser\NativeMessagingHosts\com.thunderload.host",
  "HKCU:\Software\Vivaldi\NativeMessagingHosts\com.thunderload.host",
  # Opera shares Chrome's key (Opera reads Chrome's native-messaging
  # key); the legacy Opera key is removed too in case an older
  # installer wrote it.
  "HKCU:\Software\Opera Software\NativeMessagingHosts\com.thunderload.host",
  "HKCU:\Software\Mozilla\NativeMessagingHosts\com.thunderload.host"
)
foreach ($Key in $Keys) {
  if (Test-Path $Key) {
    try { Remove-Item -Path $Key -Recurse -Force -ErrorAction Stop }
    catch { $Failures += "$Key : $($_.Exception.Message)" }
  }
}
$InstallDir = Join-Path $env:LOCALAPPDATA "ThunderLoad\native-host"
if (Test-Path $InstallDir) {
  try { Remove-Item -Path $InstallDir -Recurse -Force -ErrorAction Stop }
  catch { $Failures += "$InstallDir : $($_.Exception.Message)" }
}
if ($Failures.Count -gt 0) {
  throw ("ThunderLoad browser native host removal FAILED:`n" + ($Failures -join "`n"))
}
Write-Host "ThunderLoad browser native host registrations removed."
