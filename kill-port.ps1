param (
    [Parameter(Mandatory=$true)]
    [int]$Port
)

Write-Host "Searching for process using port $Port..."

try {
    $connections = Get-NetTCPConnection -LocalPort $Port -ErrorAction Stop
    
    if ($connections) {
        $processId = $connections.OwningProcess
        $process = Get-Process -Id $processId
        
        Write-Host "Found process using port ${Port}:" -ForegroundColor Green
        Write-Host "  Name: $($process.Name)" -ForegroundColor Green
        Write-Host "  PID:  $processId" -ForegroundColor Green
        Write-Host "  Path: $($process.Path)" -ForegroundColor Green
        
        $confirmation = Read-Host "Do you want to kill this process? (Y/N)"
        
        if ($confirmation -eq 'Y' -or $confirmation -eq 'y') {
            Write-Host "Stopping process..." -ForegroundColor Yellow
            Stop-Process -Id $processId -Force
            Write-Host "Process successfully terminated." -ForegroundColor Green
        }
        else {
            Write-Host "Operation cancelled." -ForegroundColor Yellow
        }
    }
    else {
        Write-Host "No process found using port $Port." -ForegroundColor Yellow
    }
}
catch [Microsoft.PowerShell.Cmdletization.Cim.CimJobException] {
    Write-Host "No process found using port $Port." -ForegroundColor Yellow
}
catch {
    Write-Host "An error occurred: $_" -ForegroundColor Red
}