Option Explicit

Dim objShell, fso, projectDir, venvPython, cmd
Set objShell = CreateObject("WScript.Shell")
Set fso = CreateObject("Scripting.FileSystemObject")

projectDir = fso.GetParentFolderName(WScript.ScriptFullName)
venvPython = projectDir & "\\.venv\\Scripts\\python.exe"

If fso.FileExists(venvPython) Then
    cmd = Chr(34) & venvPython & Chr(34) & " -m V3.app"
Else
    cmd = "python -m V3.app"
End If

objShell.CurrentDirectory = projectDir
objShell.Run cmd, 0, False
