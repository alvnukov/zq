!include "MUI2.nsh"
!include "LogicLib.nsh"
!include "WinMessages.nsh"

!ifndef VERSION
  !define VERSION "0.0.0-dev"
!endif

!ifndef INPUT_BIN
  !error "INPUT_BIN define is required"
!endif

!ifndef OUTPUT_EXE
  !define OUTPUT_EXE "dist\zq_windows_amd64_installer.exe"
!endif

!define APP_NAME "zq"
!define UNINSTALL_KEY "Software\Microsoft\Windows\CurrentVersion\Uninstall\zq"
!define APP_REG_KEY "Software\zq"
!define ENV_REG_KEY "SYSTEM\CurrentControlSet\Control\Session Manager\Environment"
!define MUI_FINISHPAGE_NOAUTOCLOSE

Name "${APP_NAME}"
OutFile "${OUTPUT_EXE}"
Unicode True
RequestExecutionLevel admin
InstallDir "$PROGRAMFILES64\zq"
InstallDirRegKey HKLM "${APP_REG_KEY}" "InstallDir"
BrandingText "${APP_NAME} ${VERSION}"

!define MUI_ABORTWARNING
!insertmacro MUI_PAGE_WELCOME
!insertmacro MUI_PAGE_COMPONENTS
!insertmacro MUI_PAGE_DIRECTORY
!insertmacro MUI_PAGE_INSTFILES
!insertmacro MUI_PAGE_FINISH

!insertmacro MUI_UNPAGE_CONFIRM
!insertmacro MUI_UNPAGE_INSTFILES

!insertmacro MUI_LANGUAGE "English"

Section "Core files (required)" SecCore
  SectionIn RO
  SetRegView 64
  SetOutPath "$INSTDIR"
  File "/oname=zq.exe" "${INPUT_BIN}"
  WriteUninstaller "$INSTDIR\Uninstall.exe"

  WriteRegStr HKLM "${APP_REG_KEY}" "InstallDir" "$INSTDIR"

  WriteRegStr HKLM "${UNINSTALL_KEY}" "DisplayName" "${APP_NAME}"
  WriteRegStr HKLM "${UNINSTALL_KEY}" "DisplayVersion" "${VERSION}"
  WriteRegStr HKLM "${UNINSTALL_KEY}" "Publisher" "alvnukov"
  WriteRegStr HKLM "${UNINSTALL_KEY}" "InstallLocation" "$INSTDIR"
  WriteRegStr HKLM "${UNINSTALL_KEY}" "UninstallString" "$\"$INSTDIR\Uninstall.exe$\""
  WriteRegDWORD HKLM "${UNINSTALL_KEY}" "NoModify" 1
  WriteRegDWORD HKLM "${UNINSTALL_KEY}" "NoRepair" 1
SectionEnd

Section "Add zq to PATH (recommended)" SecPath
  SetRegView 64
  Call AddInstallDirToPath
SectionEnd

Section "Uninstall"
  SetRegView 64
  Call un.RemoveInstallDirFromPath
  Delete "$INSTDIR\zq.exe"
  Delete "$INSTDIR\Uninstall.exe"
  RMDir "$INSTDIR"

  DeleteRegKey HKLM "${APP_REG_KEY}"
  DeleteRegKey HKLM "${UNINSTALL_KEY}"
SectionEnd

!insertmacro MUI_FUNCTION_DESCRIPTION_BEGIN
  !insertmacro MUI_DESCRIPTION_TEXT ${SecCore} "Install zq CLI binary."
  !insertmacro MUI_DESCRIPTION_TEXT ${SecPath} "Add zq install directory to system PATH."
!insertmacro MUI_FUNCTION_DESCRIPTION_END

Function RefreshEnvironment
  System::Call 'user32::SendMessageTimeoutW(i ${HWND_BROADCAST}, i ${WM_SETTINGCHANGE}, i 0, w "Environment", i 0x2, i 5000, *i .r0)'
FunctionEnd

Function AddInstallDirToPath
  ReadRegStr $0 HKLM "${ENV_REG_KEY}" "Path"
  ${If} $0 == ""
    StrCpy $0 "$INSTDIR"
  ${ElseIf} $0 == "$INSTDIR"
    Return
  ${Else}
    StrCpy $0 "$0;$INSTDIR"
  ${EndIf}
  WriteRegExpandStr HKLM "${ENV_REG_KEY}" "Path" "$0"
  Call RefreshEnvironment
FunctionEnd

Function un.RemoveInstallDirFromPath
  ; Intentionally no-op for PATH cleanup to avoid fragile string macro dependencies
  ; across NSIS variants used by CI runners.
FunctionEnd

Function un.RefreshEnvironment
  System::Call 'user32::SendMessageTimeoutW(i ${HWND_BROADCAST}, i ${WM_SETTINGCHANGE}, i 0, w "Environment", i 0x2, i 5000, *i .r0)'
FunctionEnd
