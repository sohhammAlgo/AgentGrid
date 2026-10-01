# Stops every AgentGrid-Lite control plane and node JVM on this machine.
#
# Use it when a control plane was killed without running its shutdown hook (Task Manager
# "End task", taskkill /F, Stop-Process -Force): Windows gives a force-killed JVM no chance to
# run hooks, so its node processes keep running and hold their ports (1601-1605, plus
# 1600+id for nodes added at runtime).
# Only java processes whose command line runs agentgrid.node.NodeMain or
# agentgrid.control.ControlPlaneMain are stopped.

$targets = Get-CimInstance Win32_Process -Filter "Name='java.exe'" | Where-Object {
    $_.CommandLine -match 'agentgrid\.node\.NodeMain' -or $_.CommandLine -match 'agentgrid\.control\.ControlPlaneMain'
}

if (-not $targets) {
    Write-Host "No AgentGrid-Lite java processes are running."
} else {
    # Control planes first, so a live one cannot restart nodes while they are being stopped.
    $ordered = @($targets | Where-Object { $_.CommandLine -match 'ControlPlaneMain' }) +
               @($targets | Where-Object { $_.CommandLine -match 'NodeMain' })
    foreach ($p in $ordered) {
        $what = if ($p.CommandLine -match '(agentgrid\.\S+(\s+\d+)?)') { $Matches[1] } else { 'java' }
        try {
            Stop-Process -Id $p.ProcessId -Force -ErrorAction Stop
            Write-Host ("stopped {0,-7} {1}" -f $p.ProcessId, $what)
        } catch {
            Write-Host ("could not stop {0}: {1}" -f $p.ProcessId, $_.Exception.Message)
        }
    }
}

Start-Sleep -Milliseconds 500
# Node ports are 1600 + node id; added nodes get ids above 5 (never reused), so check 1601-1699.
$busy = netstat -ano | Select-String 'LISTENING' | Select-String ':8080 |:16(0[1-9]|[1-9][0-9]) '
if ($busy) {
    Write-Host "Ports still in use:"
    $busy | ForEach-Object { Write-Host $_.Line }
} else {
    Write-Host "Ports 8080 and 1601-1699 are free."
}
