# PowerShell script to build base images for Flink and Spark
$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = Split-Path -Parent (Split-Path -Parent $ScriptDir)

Write-Host "Building base images for streaming platforms..."

Write-Host "Building Flink base image..."
docker build `
  -f "$ProjectRoot\infra\docker\flink-base.Dockerfile" `
  -t arch-flink:1.20.0 `
  $ProjectRoot

if ($LASTEXITCODE -ne 0) {
    Write-Host "Failed to build Flink image"
    exit 1
}

Write-Host "Building Spark base image..."
docker build `
  -f "$ProjectRoot\infra\docker\spark-base.Dockerfile" `
  -t arch-spark:3.5.0 `
  $ProjectRoot

if ($LASTEXITCODE -ne 0) {
    Write-Host "Failed to build Spark image"
    exit 1
}

Write-Host "Base images built successfully!"
docker images | Select-String -Pattern 'arch-flink|arch-spark'