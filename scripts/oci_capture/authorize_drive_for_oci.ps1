[CmdletBinding()]
param(
    [ValidatePattern('^trendguard-osaka-a1$')]
    [string]$SshHost = 'trendguard-osaka-a1'
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$remoteInstaller = 'sudo -n /usr/local/libexec/install_pump_fade_v3_rclone_config'
$enableTimer = 'sudo -n systemctl enable --now pump-fade-v3-uploader.timer'

try {
    $null = Get-Command rclone -CommandType Application -ErrorAction Stop
    $scope = [Convert]::ToBase64String(
        [System.Text.Encoding]::UTF8.GetBytes('{"scope":"drive.file"}')
    )
    $authorizationOutput = @(& rclone authorize drive $scope 2>$null)
    if ($LASTEXITCODE -ne 0) {
        Write-Output 'FAIL'
        exit 1
    }
    $authorizationText = $authorizationOutput -join "`n"
    $jsonStart = $authorizationText.IndexOf('{')
    $jsonEnd = $authorizationText.LastIndexOf('}')
    if ($jsonStart -lt 0 -or $jsonEnd -le $jsonStart) {
        Write-Output 'FAIL'
        exit 1
    }
    $tokenJson = $authorizationText.Substring($jsonStart, $jsonEnd - $jsonStart + 1)
    $token = $tokenJson | ConvertFrom-Json -ErrorAction Stop
    if ([string]::IsNullOrWhiteSpace($token.access_token) -or
        [string]::IsNullOrWhiteSpace($token.refresh_token) -or
        [string]::IsNullOrWhiteSpace($token.token_type)) {
        Write-Output 'FAIL'
        exit 1
    }

    $remoteResult = @(
        $tokenJson | & ssh -o BatchMode=yes -o StrictHostKeyChecking=yes $SshHost $remoteInstaller 2>$null
    )
    if ($LASTEXITCODE -ne 0 -or $remoteResult.Count -ne 1 -or $remoteResult[0] -cne 'OK') {
        Write-Output 'FAIL'
        exit 1
    }

    $timerResult = @(
        & ssh -o BatchMode=yes -o StrictHostKeyChecking=yes $SshHost $enableTimer 2>$null
    )
    if ($LASTEXITCODE -ne 0) {
        Write-Output 'FAIL'
        exit 1
    }
    Write-Output 'OK'
    exit 0
} catch {
    Write-Output 'FAIL'
    exit 1
}
