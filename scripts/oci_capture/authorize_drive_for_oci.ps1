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

# 2) Own Google OAuth client (rclone's shared default client hits rateLimitExceeded).
#    The client secret is read hidden and never printed.
$clientId = (Read-Host '구글 OAuth 클라이언트 ID 붙여넣기 (…apps.googleusercontent.com)').Trim()
if ($clientId -notmatch '^[0-9]+-[a-z0-9]+\.apps\.googleusercontent\.com$') { Fail 'client-id' 'client ID format is wrong' }
$sec = Read-Host '클라이언트 보안 비밀번호 붙여넣기 (화면에 안 보임)' -AsSecureString
$bstr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($sec)
try { $clientSecret = [Runtime.InteropServices.Marshal]::PtrToStringBSTR($bstr).Trim() }
finally { [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr) }
$sec = $null
if (-not $clientSecret) { Fail 'client-secret' 'empty client secret' }
$cfgJson = '{"scope":"drive.file","client_id":' + (ConvertTo-Json $clientId) + ',"client_secret":' + (ConvertTo-Json $clientSecret) + '}'

Write-Host '브라우저가 열리면 구글 계정을 선택하고 "허용"을 누르세요...' -ForegroundColor Cyan
# rclone expects unpadded URL-safe base64 (RawURLEncoding) for the config blob.
$scope = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($cfgJson)).TrimEnd('=').Replace('+', '-').Replace('/', '_')
$cfgJson = $null
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

# Old rclone prints the token JSON directly. Newer rclone, given a base64 config blob,
# answers with a base64url blob of {"token":"<token JSON>"} between the paste markers.
function Get-Prop($obj, [string]$name) {
    $p = $obj.PSObject.Properties[$name]; if ($p) { return $p.Value } else { return $null }
}
$tokenJson = $null
if ($out -match '--->\s*([A-Za-z0-9_-]{20,})\s*<---') {
    $b64 = $Matches[1].Replace('-', '+').Replace('_', '/')
    $b64 += '=' * ((4 - $b64.Length % 4) % 4)
    try { $decoded = [Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($b64)) | ConvertFrom-Json }
    catch { Fail 'token-parse' 'rclone output blob is not valid base64 JSON' }
    $inner = Get-Prop $decoded 'token'
    if ($inner -is [string]) { $tokenJson = $inner }
    elseif (Get-Prop $decoded 'access_token') { $tokenJson = $decoded | ConvertTo-Json -Compress }
    $decoded = $null; $inner = $null; $b64 = $null
} else {
    $s = $out.IndexOf('{'); $e = $out.LastIndexOf('}')
    if ($s -ge 0 -and $e -gt $s) { $tokenJson = $out.Substring($s, $e - $s + 1) }
}
$out = $null
if (-not $tokenJson) { Fail 'token-parse' 'no token found in rclone output' }
try { $token = $tokenJson | ConvertFrom-Json } catch { Fail 'token-parse' 'token is not valid JSON' }
if (-not (Get-Prop $token 'access_token') -or -not (Get-Prop $token 'refresh_token')) { Fail 'token-parse' 'token missing fields' }
$token = $null
$tokenJson = ($tokenJson -replace '[\r\n]', '')
$payload = '{"client_id":' + (ConvertTo-Json $clientId) + ',"client_secret":' + (ConvertTo-Json $clientSecret) + ',"token":' + $tokenJson + '}'
$clientSecret = $null; $scope = $null; $tokenJson = $null

# 3) Send client + token over SSH stdin to the remote installer
$remote = $payload | & ssh -o BatchMode=yes -o ConnectTimeout=15 $SshHost $remoteInstaller 2>&1
$rc = $LASTEXITCODE
$payload = $null
if ($rc -ne 0 -or (($remote | Out-String).Trim() -cne 'OK')) {
    Fail 'remote-install' ("exit ${rc}: " + (($remote | Out-String).Trim()))
}

Write-Host 'OK — OCI Drive 인증 완료 (업로더 타이머는 Luna가 검증 후 켭니다)' -ForegroundColor Green
exit 0
