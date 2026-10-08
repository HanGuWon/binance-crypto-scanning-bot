# Manual PC-side, fail-closed transfer. Default: dry-run and never prune.
[CmdletBinding()]
param(
    [Parameter(Mandatory=$true)][ValidatePattern('^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$')][string]$SshHost,
    [ValidatePattern('^/var/lib/binance-bot-2/prospective-r/pump-fade-v3/capture$')][string]$RemoteDirectory = '/var/lib/binance-bot-2/prospective-r/pump-fade-v3/capture',
    [string]$Destination = 'D:\Binance-trade-analysis\oci-capture',
    [switch]$Execute,
    [switch]$PruneVerified
)
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
if ($PruneVerified -and -not $Execute) { throw 'PruneVerified requires Execute.' }
if ($RemoteDirectory -match '(^|/)\.\.?(/|$)|//|/$') { throw 'Unsafe remote directory.' }
if ($RemoteDirectory -ne '/var/lib/binance-bot-2/prospective-r/pump-fade-v3/capture') {
    throw 'Remote directory must match the dedicated audited capture root.'
}
if ($Destination -notmatch '^[A-Za-z]:\\') { throw 'Destination must be an absolute Windows path.' }
$resolvedDestination = [System.IO.Path]::GetFullPath($Destination).TrimEnd('\')
if ($resolvedDestination -notmatch '^[A-Za-z]:\\Binance-trade-analysis\\oci-capture(?:\\|$)') {
    throw 'Destination must remain within the PC capture archive root.'
}
# Never rewrite ACLs. The operator must provide exclusive archive ownership.
# Reject an already-existing reparse/junction ancestor before copying or prune.
$ancestor = $resolvedDestination
while ($ancestor) {
    if (Test-Path -LiteralPath $ancestor) {
        if ((Get-Item -LiteralPath $ancestor -Force).Attributes -band [System.IO.FileAttributes]::ReparsePoint) {
            throw 'Reparse ancestor of capture archive destination.'
        }
    }
    $parent = [System.IO.Path]::GetDirectoryName($ancestor)
    if (-not $parent -or $parent -eq $ancestor) { break }
    $ancestor = $parent
}
$nowMs = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
$cutoffMs = $nowMs - 7L * 24 * 60 * 60 * 1000
$namePattern = '^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,180}\.manifest\.json$'
$generationPattern = '^capture-[A-Za-z0-9-]{1,100}-g[0-9]{4}$'

function Invoke-SshChecked([string]$Script) {
    $result = & ssh -o BatchMode=yes -o ConnectTimeout=10 -- $SshHost $Script
    if ($LASTEXITCODE -ne 0) { throw "SSH command failed ($LASTEXITCODE)." }
    return $result
}
function Get-RemoteDigest([string]$Name) {
    if ($Name -notmatch '^(capture-[A-Za-z0-9-]{1,100}-g[0-9]{4}/)?[a-zA-Z0-9][a-zA-Z0-9_.-]{0,220}$') { throw 'Unsafe segment filename.' }
    $result = @(Invoke-SshChecked "cd '$RemoteDirectory' && test ! -L '$Name' && test -f '$Name' && sha256sum -- '$Name'")
    if ($result.Count -ne 1 -or $result[0] -notmatch '^([a-fA-F0-9]{64})\s+') { throw 'Invalid remote digest response.' }
    return $Matches[1].ToLowerInvariant()
}

# Entire prune transaction is one remote operation. The Python program is sent
# as base64-encoded source, so no local path or manifest text becomes shell code.
$pruneHelper = @'
import hashlib
import json
import os
import re
import stat
import sys
import time

root, generation, data_name, manifest_name, data_hash, manifest_hash, start_hash, terminal_hash = sys.argv[1:]
assert root == "/var/lib/binance-bot-2/prospective-r/pump-fade-v3/capture"
assert re.fullmatch(r"capture-[A-Za-z0-9-]{1,100}-g[0-9]{4}", generation)
assert re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,180}", data_name)
assert manifest_name == data_name + ".manifest.json"
assert re.fullmatch(r"[a-f0-9]{64}", data_hash)
assert re.fullmatch(r"[a-f0-9]{64}", manifest_hash)
flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
root_fd = os.open(root, flags)
generation_fd = None
stage_fd = None
try:
    generation_fd = os.open(generation, flags, dir_fd=root_fd)
    def digest_file(dir_fd, name):
        fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=dir_fd)
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                raise RuntimeError("unsafe or multiply-linked segment")
            sha = hashlib.sha256()
            contents = bytearray() if name.endswith((".manifest.json", "generation-terminal.json", "generation-receipt.json")) else None
            while True:
                part = os.read(fd, 1048576)
                if not part:
                    break
                if contents is not None:
                    contents.extend(part)
                    if len(contents) > 65536:
                        raise RuntimeError("oversized manifest")
                sha.update(part)
            return sha.hexdigest(), bytes(contents) if contents is not None else None
        finally:
            os.close(fd)
    actual_start, _ = digest_file(generation_fd, "generation-receipt.json")
    actual_terminal, terminal_raw = digest_file(generation_fd, "generation-terminal.json")
    if actual_start != start_hash or actual_terminal != terminal_hash:
        raise RuntimeError("generation receipts changed")
    terminal = json.loads(terminal_raw)
    if (terminal.get("schema") != "pump_fade_v3_generation_terminal_v1"
            or terminal.get("status") != "CLEAN_CLOSED"
            or terminal.get("recovered_crash") is not False
            or terminal.get("archive_clean") is not True
            or terminal.get("generation_directory") != generation
            or terminal.get("start_sha256") != start_hash
            or not re.fullmatch(r"[a-f0-9]{64}", str(terminal.get("plan_sha256", "")))
            or not isinstance(terminal.get("process_boot_id"), str)
            or type(terminal.get("terminal_at_ms")) is not int
            or type(terminal.get("compressed_bytes_by_stream")) is not dict):
        raise RuntimeError("generation not cleanly closed")
    observed_manifest, raw = digest_file(generation_fd, manifest_name)
    observed_data, _ = digest_file(generation_fd, data_name)
    if (observed_manifest, observed_data) != (manifest_hash, data_hash):
        raise RuntimeError("remote hash changed")
    manifest = json.loads(raw)
    if manifest.get("data_file") != data_name or manifest.get("sha256") != data_hash:
        raise RuntimeError("manifest binding mismatch")
    receipt = manifest.get("last_received_at_ms")
    if type(receipt) is not int or receipt >= int(time.time() * 1000) - 7 * 86400000:
        raise RuntimeError("newest seven days protected")
    try:
        os.mkdir(".prune-staging", mode=0o700, dir_fd=generation_fd)
    except FileExistsError:
        pass
    stage_fd = os.open(".prune-staging", flags, dir_fd=generation_fd)
    if os.fstat(stage_fd).st_dev != os.fstat(generation_fd).st_dev:
        raise RuntimeError("cross-filesystem quarantine forbidden")
    quarantine = manifest_hash
    os.mkdir(quarantine, mode=0o700, dir_fd=stage_fd)
    quarantine_fd = os.open(quarantine, flags, dir_fd=stage_fd)
    try:
        # Rename each item only after hash and source checks; on any error,
        # preserve the quarantine and leave recovery to a human operator.
        os.rename(data_name, data_name, src_dir_fd=generation_fd, dst_dir_fd=quarantine_fd)
        os.rename(manifest_name, manifest_name, src_dir_fd=generation_fd, dst_dir_fd=quarantine_fd)
        if digest_file(quarantine_fd, data_name)[0] != data_hash or digest_file(quarantine_fd, manifest_name)[0] != manifest_hash:
            raise RuntimeError("post-rename hash mismatch; quarantine retained")
        os.unlink(data_name, dir_fd=quarantine_fd)
        os.unlink(manifest_name, dir_fd=quarantine_fd)
        os.rmdir(quarantine, dir_fd=stage_fd)
        print("PRUNED_VERIFIED")
    finally:
        os.close(quarantine_fd)
finally:
    if stage_fd is not None:
        os.close(stage_fd)
    if generation_fd is not None:
        os.close(generation_fd)
    os.close(root_fd)
'@

function Invoke-RemoteVerifiedPrune([string]$Generation, [string]$DataName, [string]$ManifestName, [string]$DataHash, [string]$ManifestHash, [string]$StartHash, [string]$TerminalHash) {
    $encoded = [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($pruneHelper))
    $command = "python3 -c 'exec(__import__(`"base64`").b64decode(`"$encoded`"))' '$RemoteDirectory' '$Generation' '$DataName' '$ManifestName' '$DataHash' '$ManifestHash' '$StartHash' '$TerminalHash'"
    $receipt = @(Invoke-SshChecked $command)
    if ($receipt.Count -ne 1 -or $receipt[0] -cne 'PRUNED_VERIFIED') {
        throw 'Remote prune did not return its exact success receipt.'
    }
}

function Copy-VerifiedReceipt([string]$Generation, [string]$Filename) {
    $relative = "$Generation/$Filename"
    $remoteHash = Get-RemoteDigest $relative
    if (-not $Execute) { return $remoteHash }
    $destinationDirectory = Join-Path $resolvedDestination $Generation
    $null = New-Item -ItemType Directory -Path $destinationDirectory -Force
    if ((Get-Item -LiteralPath $destinationDirectory -Force).Attributes -band [System.IO.FileAttributes]::ReparsePoint) { throw 'Reparse generation directory.' }
    $destinationFile = Join-Path $destinationDirectory $Filename
    if (Test-Path -LiteralPath $destinationFile) {
        if ((Get-Item -LiteralPath $destinationFile -Force).Attributes -band [System.IO.FileAttributes]::ReparsePoint) { throw 'Reparse receipt.' }
    } else {
        $temporary = "$destinationFile.transfer-part"
        if (Test-Path -LiteralPath $temporary) { throw 'Stale receipt transfer.' }
        & scp -B -q -- "${SshHost}:${RemoteDirectory}/${relative}" $temporary
        if ($LASTEXITCODE -ne 0) { throw 'Receipt SCP failure.' }
        if ((Get-FileHash -LiteralPath $temporary -Algorithm SHA256).Hash.ToLowerInvariant() -cne $remoteHash) { throw 'Receipt transfer SHA mismatch.' }
        Move-Item -LiteralPath $temporary -Destination $destinationFile
    }
    if ((Get-FileHash -LiteralPath $destinationFile -Algorithm SHA256).Hash.ToLowerInvariant() -cne $remoteHash) { throw 'Receipt readback SHA mismatch.' }
    return $remoteHash
}

# Enumerate exactly one generation level. Incomplete generations are skipped.
$generations = @(Invoke-SshChecked "cd '$RemoteDirectory' && find . -mindepth 1 -maxdepth 1 -type d -name 'capture-*-g????' -printf '%f\n'")
foreach ($generation in $generations) {
    if ($generation -cnotmatch $generationPattern) { throw 'Unsafe generation name.' }
    $receiptNames = @('generation-receipt.json', 'generation-terminal.json')
    # Expected live/incomplete generations are not batch failures. An invalid
    # *present* terminal is also ineligible, not permission to prune it.
    $ready = @(Invoke-SshChecked "cd '$RemoteDirectory' && if test ! -L '$generation' && test -f '$generation/generation-terminal.json' && test ! -L '$generation/generation-terminal.json' && test -f '$generation/generation-receipt.json' && test ! -L '$generation/generation-receipt.json'; then printf 'READY'; else printf 'SKIP'; fi")
    if ($ready.Count -ne 1 -or $ready[0] -cne 'READY') {
        Write-Output "SKIP incomplete/live generation: $generation"
        continue
    }
    $terminalText = @(Invoke-SshChecked "cd '$RemoteDirectory' && test ! -L '$generation' && test -f '$generation/generation-terminal.json' && test ! -L '$generation/generation-terminal.json' && head -c 65537 -- '$generation/generation-terminal.json'") -join "`n"
    if ([System.Text.Encoding]::UTF8.GetByteCount($terminalText) -gt 65536) {
        Write-Output "SKIP oversized generation terminal: $generation"
        continue
    }
    try { $terminal = $terminalText | ConvertFrom-Json -ErrorAction Stop }
    catch {
        Write-Output "SKIP invalid generation terminal: $generation"
        continue
    }
    if ($terminal.schema -cne 'pump_fade_v3_generation_terminal_v1' -or $terminal.status -cne 'CLEAN_CLOSED' -or
        $terminal.recovered_crash -cne $false -or $terminal.archive_clean -cne $true -or
        $terminal.generation_directory -cne $generation -or
        $terminal.start_sha256 -cnotmatch '^[a-f0-9]{64}$' -or
        $terminal.plan_sha256 -cnotmatch '^[a-f0-9]{64}$' -or
        $terminal.process_boot_id -isnot [string] -or
        $terminal.terminal_at_ms -isnot [ValueType] -or
        $null -eq $terminal.compressed_bytes_by_stream) { continue }
    $startHash = Get-RemoteDigest "$generation/generation-receipt.json"
    if ($startHash -cne $terminal.start_sha256) { continue }
    $terminalHash = Get-RemoteDigest "$generation/generation-terminal.json"
    # Remote prune reopens and hashes both receipts in its one operation.
    if ($Execute) {
        $null = New-Item -ItemType Directory -Force -Path $resolvedDestination
        if ((Get-Item -LiteralPath $resolvedDestination -Force).Attributes -band [System.IO.FileAttributes]::ReparsePoint) { throw 'Reparse archive root.' }
    }
    foreach ($receiptName in $receiptNames) { $null = Copy-VerifiedReceipt $generation $receiptName }
    $names = @(Invoke-SshChecked "cd '$RemoteDirectory' && find '$generation' -mindepth 1 -maxdepth 1 -type f -name '*.manifest.json' -printf '%f\n'")
foreach ($name in $names) {
    if ($name -notmatch $namePattern) { throw "Unexpected manifest name: $name" }
    $segmentName = $name.Substring(0, $name.Length - '.manifest.json'.Length)
    if ($segmentName -notmatch '^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,180}$') { throw 'Unsafe data filename.' }
    # Read only bounded manifest JSON, without loading unsealed data bytes.
    $remoteManifestHash = Get-RemoteDigest "$generation/$name"
    $manifestJson = @(Invoke-SshChecked "cd '$RemoteDirectory' && test ! -L '$generation/$name' && test -f '$generation/$name' && head -c 65537 -- '$generation/$name'") -join "`n"
    if ([System.Text.Encoding]::UTF8.GetByteCount($manifestJson) -gt 65536) { throw 'Manifest exceeds 64 KiB.' }
    $manifest = $manifestJson | ConvertFrom-Json
    if ($manifest.data_file -cne $segmentName -or
        $manifest.sha256 -cnotmatch '^[a-f0-9]{64}$' -or
        $manifest.last_received_at_ms -isnot [ValueType] -or
        [long]$manifest.last_received_at_ms -lt 0) {
        throw "Invalid or mismatched segment manifest: $name"
    }
    if ([long]$manifest.last_received_at_ms -ge $cutoffMs) { continue }
    $remoteDataHash = Get-RemoteDigest "$generation/$segmentName"
    if ($remoteDataHash -cne $manifest.sha256) { throw "Remote data differs from manifest: $segmentName" }
    if (-not $Execute) {
        Write-Output "DRY-RUN eligible verified segment: $segmentName"
        continue
    }
    $null = New-Item -ItemType Directory -Force -Path $resolvedDestination
    $captureRoot = [System.IO.Path]::GetFullPath('D:\Binance-trade-analysis\oci-capture')
    $folder = Get-Item -LiteralPath $resolvedDestination -Force
    if ($folder.Attributes -band [System.IO.FileAttributes]::ReparsePoint -or
        $resolvedDestination -cne $captureRoot) {
        throw 'Archive destination must be the real, non-reparse capture root.'
    }
    $generationDestination = Join-Path $resolvedDestination $generation
    $tempData = Join-Path $generationDestination ($segmentName + '.transfer-part')
    $tempManifest = Join-Path $generationDestination ($name + '.transfer-part')
    $localData = Join-Path $generationDestination $segmentName
    $localManifest = Join-Path $generationDestination $name
    if ((Test-Path -LiteralPath $tempData) -or (Test-Path -LiteralPath $tempManifest)) {
        throw 'Stale transfer-part files need operator inspection.'
    }
    foreach ($candidate in @($localData, $localManifest)) {
        if (Test-Path -LiteralPath $candidate) {
            if ((Get-Item -LiteralPath $candidate -Force).Attributes -band [System.IO.FileAttributes]::ReparsePoint) {
                throw 'Refusing symlink/reparse destination file.'
            }
        }
    }
    if (-not (Test-Path -LiteralPath $localData)) {
        & scp -B -q -- "${SshHost}:${RemoteDirectory}/${generation}/${segmentName}" $tempData
        if ($LASTEXITCODE -ne 0) { throw "SCP data failure: $segmentName" }
        if ((Get-FileHash -LiteralPath $tempData -Algorithm SHA256).Hash.ToLowerInvariant() -cne $remoteDataHash) {
            throw "Transferred data hash mismatch: $segmentName"
        }
        Move-Item -LiteralPath $tempData -Destination $localData
    }
    if ((Get-FileHash -LiteralPath $localData -Algorithm SHA256).Hash.ToLowerInvariant() -cne $remoteDataHash) {
        throw "Local data readback mismatch: $segmentName"
    }
    if (-not (Test-Path -LiteralPath $localManifest)) {
        & scp -B -q -- "${SshHost}:${RemoteDirectory}/${generation}/${name}" $tempManifest
        if ($LASTEXITCODE -ne 0) { throw "SCP manifest failure: $name" }
        if ((Get-FileHash -LiteralPath $tempManifest -Algorithm SHA256).Hash.ToLowerInvariant() -cne $remoteManifestHash) {
            throw "Transferred manifest hash mismatch: $name"
        }
        Move-Item -LiteralPath $tempManifest -Destination $localManifest
    }
    if ((Get-FileHash -LiteralPath $localManifest -Algorithm SHA256).Hash.ToLowerInvariant() -cne $remoteManifestHash) {
        throw "Local manifest readback mismatch: $name"
    }
    Write-Output "VERIFIED local archive: $segmentName"
    if ($PruneVerified) {
        # Verify all four local files immediately before the remote prune call.
        # This requires exclusive ownership of the non-reparse archive tree;
        # two machines cannot provide one atomic cross-host transaction.
        $localStart = Join-Path $generationDestination 'generation-receipt.json'
        $localTerminal = Join-Path $generationDestination 'generation-terminal.json'
        foreach ($candidate in @($localData, $localManifest, $localStart, $localTerminal)) {
            if (-not (Test-Path -LiteralPath $candidate -PathType Leaf)) { throw 'Local prune evidence missing.' }
            if ((Get-Item -LiteralPath $candidate -Force).Attributes -band [System.IO.FileAttributes]::ReparsePoint) { throw 'Local prune evidence is reparse.' }
        }
        $readbacks = @(
            @($localData, $remoteDataHash), @($localManifest, $remoteManifestHash),
            @($localStart, $startHash), @($localTerminal, $terminalHash)
        )
        foreach ($pair in $readbacks) {
            if ((Get-FileHash -LiteralPath $pair[0] -Algorithm SHA256).Hash.ToLowerInvariant() -cne $pair[1]) {
                throw 'Local prune evidence changed after initial verification.'
            }
        }
        Invoke-RemoteVerifiedPrune $generation $segmentName $name $remoteDataHash $remoteManifestHash $startHash $terminalHash
        Write-Output "PRUNED verified remote pair: $segmentName"
    }
}
}
