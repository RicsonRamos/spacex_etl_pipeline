# Health Check completo do pipeline

$services = @(
    @{Name="PostgreSQL"; Url="localhost:5432"; Type="tcp"},
    @{Name="Airflow"; Url="http://localhost:8080/health"; Type="http"},
    @{Name="Metabase"; Url="http://localhost:3000/api/health"; Type="http"},
    @{Name="DBT Docs"; Url="http://localhost:8081/health"; Type="http"}
)

Write-Host "=== SpaceX ETL Health Check ===" -ForegroundColor Cyan
$allHealthy = $true

foreach ($svc in $services) {
    try {
        if ($svc.Type -eq "tcp") {
            $tcp = New-Object System.Net.Sockets.TcpClient
            $tcp.Connect("localhost", 5432)
            $tcp.Close()
            Write-Host "$($svc.Name): OK" -ForegroundColor Green
        } else {
            $resp = Invoke-WebRequest -Uri $svc.Url -TimeoutSec 5 -UseBasicParsing
            if ($resp.StatusCode -eq 200) {
                Write-Host "$($svc.Name): OK" -ForegroundColor Green
            } else {
                Write-Host " $($svc.Name): Status $($resp.StatusCode)" -ForegroundColor Yellow
                $allHealthy = $false
            }
        }
    } catch {
        Write-Host "$($svc.Name): OFFLINE" -ForegroundColor Red
        $allHealthy = $false
    }
}

Write-Host ""
if ($allHealthy) {
    Write-Host "Todos os serviços estão operacionais!" -ForegroundColor Green
} else {
    Write-Host "Alguns serviços precisam de atenção." -ForegroundColor Yellow
}