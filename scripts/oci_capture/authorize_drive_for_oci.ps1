[CmdletBinding()]
param(
    [ValidatePattern('^trendguard-osaka-a1$')]
    [string]$SshHost = 'trendguard-osaka-a1'
)

# Authorizes an OAuth desktop client for the OCI drive.file uploader.
# The client secret and token stay in this process and are sent only over TLS
# to Google and through SSH stdin to the root-owned installer. They are never
# placed in a child process command line or written to disk.

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Continue'
$remoteInstaller = 'sudo -n /usr/local/libexec/install_pump_fade_v3_rclone_config'

function Fail([string]$stage, [string]$detail = '') {
    Write-Host "FAIL [$stage] $detail" -ForegroundColor Red
    exit 1
}

if (-not (Get-Command ssh -CommandType Application -ErrorAction SilentlyContinue)) {
    Fail 'ssh' 'ssh not found on this PC'
}

# Verify SSH and the installer before collecting OAuth inputs.
$probe = & ssh -o BatchMode=yes -o ConnectTimeout=15 $SshHost 'echo CONNECTED; if ! sudo -n true 2>/dev/null; then echo NO_SUDO; elif ! sudo -n test -x /usr/local/libexec/install_pump_fade_v3_rclone_config; then echo NO_INSTALLER; else echo READY; fi' 2>&1
$probeText = (($probe | Out-String).Trim())
if (-not ($probeText -match 'CONNECTED')) { Fail 'ssh-precheck' 'cannot connect to OCI' }
if ($probeText -match 'NO_SUDO') { Fail 'ssh-precheck' 'passwordless sudo is unavailable' }
if ($probeText -match 'NO_INSTALLER') { Fail 'ssh-precheck' 'remote installer is missing' }
if (-not ($probeText -match 'READY')) { Fail 'ssh-precheck' 'remote installer preflight failed' }

$clientId = (Read-Host '구글 OAuth 클라이언트 ID 붙여넣기 (…apps.googleusercontent.com)').Trim()
if ($clientId -notmatch '^[0-9]+-[a-z0-9]+\.apps\.googleusercontent\.com$') {
    Fail 'client-id' 'client ID format is wrong'
}
$secureSecret = Read-Host '클라이언트 보안 비밀번호 붙여넣기 (화면에 안 보임)' -AsSecureString
$secretPtr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secureSecret)
try {
    $clientSecret = [Runtime.InteropServices.Marshal]::PtrToStringBSTR($secretPtr).Trim()
} finally {
    [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($secretPtr)
}
$secureSecret = $null
if (-not $clientSecret) { Fail 'client-secret' 'client secret is empty' }

# Google desktop OAuth uses a loopback redirect. Use TcpListener so the script
# does not need an HttpListener URL reservation or an additional local file.
$listener = [Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback, 0)
$listener.Start()
$port = $listener.LocalEndpoint.Port
$redirectUri = "http://127.0.0.1:$port/"
$state = [guid]::NewGuid().ToString('N')
$randomBytes = New-Object byte[] 32
$randomGenerator = [Security.Cryptography.RandomNumberGenerator]::Create()
$randomGenerator.GetBytes($randomBytes)
$codeVerifier = [Convert]::ToBase64String($randomBytes).TrimEnd('=').Replace('+', '-').Replace('/', '_')
$randomGenerator.Dispose()
$randomBytes = $null
$sha256 = [Security.Cryptography.SHA256]::Create()
$codeChallenge = [Convert]::ToBase64String($sha256.ComputeHash([Text.Encoding]::ASCII.GetBytes($codeVerifier))).TrimEnd('=').Replace('+', '-').Replace('/', '_')
$sha256.Dispose()
$authQuery = @(
    'client_id=' + [Uri]::EscapeDataString($clientId)
    'redirect_uri=' + [Uri]::EscapeDataString($redirectUri)
    'response_type=code'
    'scope=' + [Uri]::EscapeDataString('https://www.googleapis.com/auth/drive.file')
    'access_type=offline'
    'prompt=consent'
    'state=' + [Uri]::EscapeDataString($state)
    'code_challenge=' + [Uri]::EscapeDataString($codeChallenge)
    'code_challenge_method=S256'
) -join '&'
$authUri = "https://accounts.google.com/o/oauth2/v2/auth?$authQuery"

try {
    Write-Host '브라우저에서 Google 계정을 선택하고 권한을 허용하세요...' -ForegroundColor Cyan
    Start-Process -FilePath $authUri
    $acceptTask = $listener.AcceptTcpClientAsync()
    if (-not $acceptTask.Wait(180000)) { Fail 'oauth-callback' 'authorization timed out' }
    $browser = $acceptTask.Result
    $stream = $browser.GetStream()
    $reader = [IO.StreamReader]::new($stream, [Text.Encoding]::ASCII)
    $requestLine = $reader.ReadLine()
    while ($null -ne ($headerLine = $reader.ReadLine()) -and $headerLine.Length -gt 0) { }
    if (-not $requestLine) { Fail 'oauth-callback' 'empty browser callback' }
    $requestPath = ($requestLine -split ' ')[1]
    Add-Type -AssemblyName System.Web
    $callback = [Uri]::new("http://127.0.0.1:$port$requestPath")
    $query = [System.Web.HttpUtility]::ParseQueryString($callback.Query)
    $code = $query.Get('code')
    if ($query.Get('state') -cne $state -or -not $code) {
        $responseBody = [Text.Encoding]::UTF8.GetBytes('Authorization failed. You may close this tab.')
        $responseHeader = "HTTP/1.1 400 Bad Request`r`nContent-Length: $($responseBody.Length)`r`nConnection: close`r`n`r`n"
        $headerBytes = [Text.Encoding]::ASCII.GetBytes($responseHeader)
        $stream.Write($headerBytes, 0, $headerBytes.Length)
        $stream.Write($responseBody, 0, $responseBody.Length)
        Fail 'oauth-callback' 'state or authorization code did not match'
    }
    $responseBody = [Text.Encoding]::UTF8.GetBytes('Authorization complete. You may close this tab.')
    $responseHeader = "HTTP/1.1 200 OK`r`nContent-Type: text/plain; charset=utf-8`r`nContent-Length: $($responseBody.Length)`r`nConnection: close`r`n`r`n"
    $headerBytes = [Text.Encoding]::ASCII.GetBytes($responseHeader)
    $stream.Write($headerBytes, 0, $headerBytes.Length)
    $stream.Write($responseBody, 0, $responseBody.Length)
    $browser.Close()
} catch {
    Fail 'oauth-callback' 'could not complete the local browser authorization'
} finally {
    $listener.Stop()
}

try {
    $tokenResponse = Invoke-RestMethod -Method Post -Uri 'https://oauth2.googleapis.com/token' `
        -ContentType 'application/x-www-form-urlencoded' `
        -Body @{
            code = $code
            client_id = $clientId
            client_secret = $clientSecret
            redirect_uri = $redirectUri
            grant_type = 'authorization_code'
            code_verifier = $codeVerifier
        }
} catch {
    Fail 'token-exchange' 'Google token exchange failed; response details were suppressed'
}
if (
    -not $tokenResponse -or
    -not $tokenResponse.PSObject.Properties['access_token'] -or
    -not $tokenResponse.PSObject.Properties['refresh_token'] -or
    -not $tokenResponse.PSObject.Properties['expires_in'] -or
    -not $tokenResponse.access_token -or
    -not $tokenResponse.refresh_token -or
    -not $tokenResponse.token_type
) {
    Fail 'token-invalid' 'Google response did not contain the required token fields'
}
$token = @{
    access_token = [string]$tokenResponse.access_token
    refresh_token = [string]$tokenResponse.refresh_token
    token_type = [string]$tokenResponse.token_type
    expiry = [DateTime]::UtcNow.AddSeconds([int]$tokenResponse.expires_in).ToString('yyyy-MM-ddTHH:mm:ss.fffZ')
}
$payload = @{
    client_id = $clientId
    client_secret = $clientSecret
    token = $token
} | ConvertTo-Json -Compress -Depth 4
$clientSecret = $null
$codeVerifier = $null
$codeChallenge = $null
$tokenResponse = $null
$token = $null

# The payload travels only over the SSH pipe. The command line has no OAuth data.
$remote = $payload | & ssh -o BatchMode=yes -o ConnectTimeout=15 $SshHost $remoteInstaller 2>&1
$exitCode = $LASTEXITCODE
$payload = $null
if ($exitCode -ne 0 -or (($remote | Out-String).Trim() -cne 'OK')) {
    Fail 'remote-install' "exit ${exitCode}: installer did not return OK"
}

Write-Host 'OK — OCI Drive 인증 완료 (업로더 타이머는 검증 후 별도로 켭니다)' -ForegroundColor Green
exit 0
