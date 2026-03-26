Option Explicit

Dim objShell, fso, projectDir, venvPython, venvGuiPython, cmd
Set objShell = CreateObject("WScript.Shell")
Set fso = CreateObject("Scripting.FileSystemObject")

projectDir = fso.GetParentFolderName(WScript.ScriptFullName)
venvPython = projectDir & "\\.venv\\Scripts\\python.exe"
venvGuiPython = projectDir & "\\.venv_gui\\Scripts\\python.exe"

If fso.FileExists(venvGuiPython) Then
    cmd = Chr(34) & venvGuiPython & Chr(34) & " -m V3.launcher"
ElseIf fso.FileExists(venvPython) Then
    cmd = Chr(34) & venvPython & Chr(34) & " -m V3.launcher"
Else
    cmd = "python -m V3.launcher"
End If

objShell.CurrentDirectory = projectDir
objShell.Run cmd, 0, False
