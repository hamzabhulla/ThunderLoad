$ErrorActionPreference = "SilentlyContinue"
$Keys = @(
  "HKCU:\Software\Google\Chrome\NativeMessagingHosts\com.thunderload.host",
  "HKCU:\Software\Microsoft\Edge\NativeMessagingHosts\com.thunderload.host",
  "HKCU:\Software\BraveSoftware\Brave-Browser\NativeMessagingHosts\com.thunderload.host",
  "HKCU:\Software\Opera Software\NativeMessagingHosts\com.thunderload.host",
  "HKCU:\Software\Vivaldi\NativeMessagingHosts\com.thunderload.host",
  "HKCU:\Software\Mozilla\NativeMessagingHosts\com.thunderload.host"
)
foreach ($Key in $Keys) { Remove-Item -Path $Key -Recurse -Force }
Remove-Item -Path (Join-Path $env:LOCALAPPDATA "ThunderLoad\native-host") -Recurse -Force
Write-Host "ThunderLoad browser native host registrations removed."
