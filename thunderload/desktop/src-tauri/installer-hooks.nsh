; ThunderLoad NSIS installer hooks (Tauri `bundle.windows.nsis.installerHooks`).
;
; Two jobs live here:
;
; 1. The uninstall choice page. When ThunderLoad is uninstalled from
;    Windows, the uninstaller asks what to remove: just the app, or the
;    app with all its components. The page is declared in this file (the
;    hooks file is included before the template's own pages, so this page
;    shows first in the uninstaller, ahead of the confirm page). The safe
;    option - just the app - is pre-selected, and silent, passive and
;    update uninstalls never show the page at all, so an upgrade or a
;    scripted uninstall can never wipe the components by accident.
;
; 2. NSIS_HOOK_PREUNINSTALL carries out the choice. It runs BEFORE any
;    files are removed, so the bundled unregister script is still on
;    disk. The hook never aborts the uninstall: a failing component
;    removal still lets the uninstall finish - a leftover file is
;    harmless, a blocked uninstall is not.
;
; What lives where (Windows, per-user install, $INSTDIR is
; %LOCALAPPDATA%\ThunderLoad):
;   - App files, shortcuts, Add/Remove Programs entry: removed by the
;     template's Section Uninstall for BOTH options.
;   - Tools (%LOCALAPPDATA%\ThunderLoad\tools: yt-dlp.exe, deno.exe, hash
;     sidecars, temp and checkpoint files): the template never touches
;     this folder, so a stock uninstall orphans it. Option 2 removes it.
;   - Native host (%LOCALAPPDATA%\ThunderLoad\native-host: versioned host
;     binaries and per-browser manifests) and the six
;     NativeMessagingHosts registry keys: removed by the bundled
;     unregister script, option 2 only. Option 1 leaves the registration
;     in place, so a browser extension that stays installed keeps
;     working with the host.
;   - App data (%APPDATA%\com.thunderload.desktop: jobs.json and the
;     WebView profile holding in-app settings): the template removes
;     these when its "Delete app data" checkbox is ticked. Option 2
;     forces that checkbox on; option 1 leaves the checkbox alone.
;   - The browser extension itself: no uninstaller can remove it (the
;     browser owns installed extensions; a user-installed extension has
;     no registry entry and no one-tap removal path). Option 2
;     disconnects it by removing the native-host registration, and the
;     choice page tells the user how to remove it from the browser.
;   - Never touched by either option: the user's downloaded files, and
;     the shared WebView2 runtime (a Microsoft component, not ours).
;
; Plain StrCmp is used instead of LogicLib so the hook does not depend
; on which includes the generated installer.nsi pulls in. ${GetOptions}
; (FileFunc.nsh) is the one exception: the template includes FileFunc
; before this hooks file, and it is the only way to read /P and /UPDATE
; off the command line without the template's $PassiveMode/$UpdateMode
; variables, which are declared after this file is included. The
; NSIS_HOOK_PREUNINSTALL macro below is the exception in the other
; direction: it is expanded inside Section Uninstall, after every
; template variable is declared, so it may read $UpdateMode and
; $DeleteAppDataCheckboxState.

; 0 = remove just the app (the safe default - pre-selected on the page,
; and the value used for silent, passive and update uninstalls, where the
; page is never shown). 1 = remove the app and all its components.
Var ThunderloadRemoveAll

Var ThunderloadRadioAppOnly
Var ThunderloadRadioAll

UninstPage custom un.ThunderloadRemoveChoice un.LeaveThunderloadRemoveChoice

Function un.ThunderloadRemoveChoice
  ; Silent, passive and update uninstalls never show the page. An update
  ; runs this uninstaller with /UPDATE and must never wipe the tools or
  ; the native host the new version is about to use.
  IfSilent thunderload_choice_skip
  ${GetOptions} $CMDLINE "/P" $0
  IfErrors thunderload_choice_check_update
  Goto thunderload_choice_skip
  thunderload_choice_check_update:
  ${GetOptions} $CMDLINE "/UPDATE" $0
  IfErrors thunderload_choice_show
  Goto thunderload_choice_skip
  thunderload_choice_show:
  !insertmacro MUI_HEADER_TEXT "Uninstall ThunderLoad" "Choose what to remove"
  nsDialogs::Create 1018
  Pop $0
  StrCmp $0 "error" thunderload_choice_skip
  ${NSD_CreateLabel} 0 0 100% 12u "Choose what should be removed with ThunderLoad."
  Pop $1
  ${NSD_CreateRadioButton} 10u 18u 100% 10u "Only the ThunderLoad app"
  Pop $ThunderloadRadioAppOnly
  ${NSD_CreateLabel} 28u 30u -10u 24u "Removes the app, its shortcuts and the Add or remove$\nprograms entry. yt-dlp, Deno, the browser extension,$\nthe native-messaging host and your jobs stay."
  Pop $1
  ${NSD_CreateRadioButton} 10u 58u 100% 10u "The app and all its components"
  Pop $ThunderloadRadioAll
  ${NSD_CreateLabel} 28u 70u -10u 32u "Also removes yt-dlp and Deno, the native-messaging host,$\nyour jobs and settings. The browser extension itself$\ncannot be removed here - remove it in the browser:$\nChrome/Edge: chrome://extensions, Firefox: about:addons."
  Pop $1
  ; Safe default pre-selected: an accidental Enter removes just the app.
  SendMessage $ThunderloadRadioAppOnly ${BM_SETCHECK} ${BST_CHECKED} 0
  ${NSD_SetFocus} $ThunderloadRadioAppOnly
  nsDialogs::Show
  Return
  thunderload_choice_skip:
  Abort
FunctionEnd

Function un.LeaveThunderloadRemoveChoice
  ${NSD_GetState} $ThunderloadRadioAll $0
  StrCmp $0 ${BST_CHECKED} thunderload_choice_all
  StrCpy $ThunderloadRemoveAll 0
  Return
  thunderload_choice_all:
  StrCpy $ThunderloadRemoveAll 1
FunctionEnd

!macro NSIS_HOOK_PREUNINSTALL
  ; Update and silent uninstalls never remove components (belt and
  ; braces on top of the page never showing for them).
  IfSilent thunderload_hook_app_only
  StrCmp $UpdateMode "1" thunderload_hook_app_only
  StrCmp $ThunderloadRemoveAll "1" thunderload_hook_remove_all thunderload_hook_app_only
  thunderload_hook_remove_all:
  ; Option 2: the template deletes the app data when its "Delete app
  ; data" checkbox is ticked; force it on so jobs and settings go with
  ; the components, whatever the checkbox shows.
  StrCpy $DeleteAppDataCheckboxState 1
  nsExec::ExecToLog 'powershell.exe -NoProfile -NonInteractive -WindowStyle Hidden -Command "Get-ScheduledTask -TaskName $\'ThunderLoad-*$\' -ErrorAction SilentlyContinue | Unregister-ScheduledTask -Confirm:$$false"'
  DetailPrint "Removing ThunderLoad components (tools, native host, app data)..."
  ; The script fails loudly on its own when a removal fails; here its
  ; exit code is only observed, never fatal to the uninstall.
  ; nsExec leaves the exit code on top of the stack, then the output.
  nsExec::ExecToStack 'powershell.exe -NoProfile -NonInteractive -WindowStyle Hidden -ExecutionPolicy Bypass -File "$INSTDIR\native-host\unregister-native-host.ps1"'
  Pop $0
  Pop $1
  StrCmp $0 "0" thunderload_hook_remove_tools
  ; Fallback when the script is gone or failed (pre-hook installs,
  ; manual delete): remove the registry keys directly, and the
  ; native-host files too, so the fallback removes the host as fully
  ; as the script does. Opera shares Chrome's key (Opera reads
  ; Chrome's native-messaging key), so there is one entry for both;
  ; the legacy Opera key is removed too in case an older installer
  ; wrote it.
  DeleteRegKey HKCU "Software\Google\Chrome\NativeMessagingHosts\com.thunderload.host"
  DeleteRegKey HKCU "Software\Microsoft\Edge\NativeMessagingHosts\com.thunderload.host"
  DeleteRegKey HKCU "Software\BraveSoftware\Brave-Browser\NativeMessagingHosts\com.thunderload.host"
  DeleteRegKey HKCU "Software\Vivaldi\NativeMessagingHosts\com.thunderload.host"
  DeleteRegKey HKCU "Software\Opera Software\NativeMessagingHosts\com.thunderload.host"
  DeleteRegKey HKCU "Software\Mozilla\NativeMessagingHosts\com.thunderload.host"
  RMDir /r "$LOCALAPPDATA\ThunderLoad\native-host"
  thunderload_hook_remove_tools:
  ; Tools the app fetched (yt-dlp, Deno, hash sidecars, temp and
  ; checkpoint files): the template never touches this folder. The
  ; tools live under LOCALAPPDATA, never under a custom $INSTDIR (the
  ; engine hard-codes %LOCALAPPDATA%\ThunderLoad\tools). RMDir /r on
  ; a missing folder is a no-op.
  RMDir /r "$LOCALAPPDATA\ThunderLoad\tools"
  Goto thunderload_hook_done
  thunderload_hook_app_only:
  ; Option 1 (and silent/update): the tools, the native host and the
  ; app data stay exactly as they are. The template's own uninstall
  ; still removes the app files, shortcuts and registry entries.
  thunderload_hook_done:
!macroend
