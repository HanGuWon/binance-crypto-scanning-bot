[CmdletBinding()]
param(
    [ValidatePattern('^trendguard-osaka-a1$')]
    [string]$SshHost = 'trendguard-osaka-a1'
)
# Authorizes a drive.file-scoped rclone remote for the OCI uploader.
# The token goes PC -> SSH stdin -> remote installer. It is never printed.
# Windows PowerShell 5.1 turns native stderr into terminating errors when
# ErrorActionPreference=Stop, so native calls run with 'Continue' and are
# checked by exit code. Each failure prints the stage name (never secrets).
# The uploader timer is NOT enabled here; Luna enables it after a verified manual run.

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Continue'
$remoteInstaller = 'sudo -n /usr/local/libexec/install_pump_fade_v3_rclone_config'

function Fail([string]$stage, [string]$detail = '') {
    Write-Host "FAIL [$stage] $detail" -ForegroundColor Red
    exit 1
}

if (-not (Get-Command rclone -CommandType Application -ErrorAction SilentlyContinue)) {
    Fail 'rclone' 'rclone not found on this PC'
}

# 1) SSH reachability and remote installer presence (no secrets involved)
$probe = & ssh -o BatchMode=yes -o ConnectTimeout=15 $SshHost 'echo CONNECTED; if ! sudo -n true 2>/dev/null; then echo NO_SUDO; elif ! sudo -n test -x /usr/local/libexec/install_pump_fade_v3_rclone_config; then echo NO_INSTALLER; else echo READY; fi' 2>&1
$probeText = (($probe | Out-String).Trim())
if (-not ($probeText -match 'CONNECTED')) { Fail 'ssh-precheck' ("cannot connect: " + $probeText) }
if ($probeText -match 'NO_SUDO') { Fail 'ssh-precheck' 'connected, but passwordless sudo is not available' }
if ($probeText -match 'NO_INSTALLER') { Fail 'ssh-precheck' 'connected, but /usr/local/libexec/install_pump_fade_v3_rclone_config is missing on OCI' }
if (-not ($probeText -match 'READY')) { Fail 'ssh-precheck' $probeText }

# 2) Browser consent for drive.file scope
Write-Host '브라우저가 열리면 구글 계정을 선택하고 "허용"을 누르세요...' -ForegroundColor Cyan
# rclone expects unpadded URL-safe base64 (RawURLEncoding) for the config blob.
$scope = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes('{"scope":"drive.file"}')).TrimEnd('=').Replace('+', '-').Replace('/', '_')
$stdoutFile = [IO.Path]::GetTempFileName()
$stderrFile = [IO.Path]::GetTempFileName()
try {
    $p = Start-Process -FilePath (Get-Command rclone).Source -ArgumentList @('authorize', 'drive', $scope) `
        -NoNewWindow -Wait -PassThru -RedirectStandardOutput $stdoutFile -RedirectStandardError $stderrFile
    $out = Get-Content -Raw -LiteralPath $stdoutFile
    if ($p.ExitCode -ne 0) {
        $err = (Get-Content -LiteralPath $stderrFile | Select-Object -Last 5) -join ' | '
        Fail 'rclone-authorize' "exit $($p.ExitCode): $err"
    }
} finally {
    Remove-Item -LiteralPath $stdoutFile, $stderrFile -Force -ErrorAction SilentlyContinue
}

$s = $out.IndexOf('{'); $e = $out.LastIndexOf('}')
if ($s -lt 0 -or $e -le $s) { Fail 'token-parse' 'no JSON token in rclone output' }
$tokenJson = $out.Substring($s, $e - $s + 1)
$out = $null
try { $token = $tokenJson | ConvertFrom-Json } catch { Fail 'token-parse' 'token is not valid JSON' }
if (-not $token.access_token -or -not $token.refresh_token) { Fail 'token-parse' 'token missing fields' }
$token = $null

# 3) Send token over SSH stdin to the remote installer
$remote = $tokenJson | & ssh -o BatchMode=yes -o ConnectTimeout=15 $SshHost $remoteInstaller 2>&1
$rc = $LASTEXITCODE
$tokenJson = $null
if ($rc -ne 0 -or (($remote | Out-String).Trim() -cne 'OK')) {
    Fail 'remote-install' ("exit ${rc}: " + (($remote | Out-String).Trim()))
}

Write-Host 'OK — OCI Drive 인증 완료 (업로더 타이머는 Luna가 검증 후 켭니다)' -ForegroundColor Green
exit 0
