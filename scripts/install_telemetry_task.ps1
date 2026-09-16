param([string]$TaskName = 'PolymarketTelemetryObservationV2')
$ErrorActionPreference = 'Stop'
$repoPath = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..')).Path
$dbPath = Join-Path $repoPath 'data\telemetry_observation_v2_20260916.sqlite3'
$launcherPath = Join-Path $PSScriptRoot 'run_telemetry_observation_v2.cmd'
if (-not (Test-Path -LiteralPath $dbPath)) {
    throw 'Prepare the new interval after both acceptance tests pass before installing this task.'
}
if (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue) {
    throw "Task $TaskName already exists; inspect it before changing a scheduled collection."
}
$operatorName = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
$action = New-ScheduledTaskAction -Execute 'cmd.exe' -Argument ('/d /c ""{0}""' -f $launcherPath) -WorkingDirectory $repoPath
$triggers = @(
    (New-ScheduledTaskTrigger -AtLogOn -User $operatorName)
)
$principal = New-ScheduledTaskPrincipal -UserId $operatorName -LogonType Interactive -RunLevel Limited
$settings = New-ScheduledTaskSettingsSet -ExecutionTimeLimit ([TimeSpan]::Zero) `
    -RestartCount 20 -RestartInterval (New-TimeSpan -Minutes 1) -MultipleInstances IgnoreNew `
    -StartWhenAvailable -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries
Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $triggers -Principal $principal `
    -Settings $settings -Description 'Approved telemetry-only observation-v2; exact-run resume after user logon, no outcomes or orders.' -ErrorAction Stop | Out-Null
Start-ScheduledTask -TaskName $TaskName -ErrorAction Stop
Get-ScheduledTask -TaskName $TaskName | Select-Object TaskName, State
