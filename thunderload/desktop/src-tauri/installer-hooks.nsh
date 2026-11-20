; ThunderLoad NSIS installer hooks (Tauri `bundle.windows.nsis.installerHooks`).
; Only the uninstall hook is used: on uninstall, the browser
; native-messaging registrations must go with the app, or Chrome/Edge/
; Brave/Opera/Firefox keep offering a host binary that no longer exists.
;
; NSIS_HOOK_PREUNINSTALL runs BEFORE any files are removed, so the
; bundled unregister script is still on disk. It lives in the install
; dir next to the exe (Tauri resources land beside the executable on
; Windows): $INSTDIR\native-host\unregister-native-host.ps1.
; The hook never aborts the uninstall: if the script is missing (older
; install, manual delete) the registry keys are removed inline instead,
; and a failing script still lets the uninstall finish — a leftover
; registry value is harmless, a blocked uninstall is not.
; Plain StrCmp is used instead of LogicLib so the hook does not depend
; on which includes the generated installer.nsi pulls in.

!macro NSIS_HOOK_PREUNINSTALL
  ; The script fails loudly on its own when a removal fails; here its
  ; exit code is only observed, never fatal to the uninstall.
  ; nsExec leaves the exit code on top of the stack, then the output.
  nsExec::ExecToStack 'powershell.exe -NoProfile -NonInteractive -WindowStyle Hidden -ExecutionPolicy Bypass -File "$INSTDIR\native-host\unregister-native-host.ps1"'
  Pop $0
  Pop $1
  StrCmp $0 "0" thunderload_uninstall_hook_done
  ; Fallback when the script is gone or failed (pre-hook installs,
  ; manual delete): remove the registry keys directly. Opera shares
  ; Chrome's key (Opera reads Chrome's native-messaging key), so there
  ; is one entry for both; the legacy Opera key is removed too in case
  ; an older installer wrote it.
  DeleteRegKey HKCU "Software\Google\Chrome\NativeMessagingHosts\com.thunderload.host"
  DeleteRegKey HKCU "Software\Microsoft\Edge\NativeMessagingHosts\com.thunderload.host"
  DeleteRegKey HKCU "Software\BraveSoftware\Brave-Browser\NativeMessagingHosts\com.thunderload.host"
  DeleteRegKey HKCU "Software\Vivaldi\NativeMessagingHosts\com.thunderload.host"
  DeleteRegKey HKCU "Software\Opera Software\NativeMessagingHosts\com.thunderload.host"
  DeleteRegKey HKCU "Software\Mozilla\NativeMessagingHosts\com.thunderload.host"
  thunderload_uninstall_hook_done:
!macroend
