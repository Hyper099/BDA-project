$ErrorActionPreference = "Stop"

$ProjectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$PythonExe = Join-Path $ProjectRoot ".venv\Scripts\python.exe"
$RawCsv = Join-Path $ProjectRoot "data\raw\financial_inclusion_data.csv"
$ModelFile = Join-Path $ProjectRoot "models\artifacts\best_model.joblib"

Write-Host "== Smart Financial Inclusion: Full Startup ==" -ForegroundColor Cyan
Write-Host "Project root: $ProjectRoot"

if (-not (Test-Path $PythonExe)) {
    throw "Python virtual environment not found at .venv. Create it first: python -m venv .venv"
}

Set-Location $ProjectRoot

if (-not (Test-Path $RawCsv)) {
    Write-Host "Raw dataset not found. Generating synthetic data..." -ForegroundColor Yellow
    & $PythonExe -m data.generate_data --rows 5000 --seed 42
}

if (-not (Test-Path $ModelFile)) {
    Write-Host "Model artifact not found. Training model..." -ForegroundColor Yellow
    & $PythonExe -m data.pyspark_pipeline --input data/raw/financial_inclusion_data.csv --output data/processed/financial_inclusion_features.csv
    & $PythonExe -m models.train_model
}

Write-Host "Starting HDFS + Spark cluster with Docker Compose..." -ForegroundColor Yellow
docker compose -f docker-compose.spark-hdfs.yml up -d

Write-Host "Waiting for NameNode safe mode to turn OFF..." -ForegroundColor Yellow
$maxAttempts = 24
$attempt = 0
while ($attempt -lt $maxAttempts) {
    $safeModeState = docker exec fi-hdfs-namenode hdfs dfsadmin -safemode get 2>$null
    if ($safeModeState -notmatch "ON") {
        break
    }
    Start-Sleep -Seconds 5
    $attempt += 1
}

if ($attempt -ge $maxAttempts) {
    Write-Host "NameNode still in safe mode; attempting to leave safe mode..." -ForegroundColor Yellow
    docker exec fi-hdfs-namenode hdfs dfsadmin -safemode leave | Out-Null
}

Write-Host "Uploading raw data to HDFS..." -ForegroundColor Yellow
docker exec fi-hdfs-namenode hdfs dfs -mkdir -p /data/raw
docker exec fi-hdfs-namenode hdfs dfs -put -f /workspace-data/raw/financial_inclusion_data.csv /data/raw/

Write-Host "Running PySpark job on Spark + HDFS..." -ForegroundColor Yellow
docker exec fi-spark /opt/spark/bin/spark-submit --master spark://fi-spark:7077 --conf spark.hadoop.fs.defaultFS=hdfs://namenode:8020 /app/scripts/dag_inspect_hdfs.py

Write-Host "Starting backend (FastAPI) in a new terminal..." -ForegroundColor Yellow
Start-Process powershell -ArgumentList @(
    "-NoExit",
    "-Command",
    "Set-Location '$ProjectRoot'; & '$PythonExe' -m uvicorn api.main:app --host 0.0.0.0 --port 8000"
)

Write-Host "Starting frontend (Streamlit) in a new terminal..." -ForegroundColor Yellow
Start-Process powershell -ArgumentList @(
    "-NoExit",
    "-Command",
    "Set-Location '$ProjectRoot'; & '$PythonExe' -m streamlit run dashboard/app.py"
)

Write-Host ""
Write-Host "Startup complete." -ForegroundColor Green
Write-Host "Backend:   http://localhost:8000"
Write-Host "Frontend:  http://localhost:8501"
Write-Host "HDFS UI:   http://localhost:9870"
Write-Host "Spark Cluster UI: http://localhost:8080"
Write-Host "Spark Local Jobs: http://localhost:4040/jobs (appears only during API-triggered local Spark session)"

function Test-UrlReachable {
    param([string]$Url)
    try {
        $resp = Invoke-WebRequest -Uri $Url -UseBasicParsing -TimeoutSec 5
        return ($resp.StatusCode -ge 200 -and $resp.StatusCode -lt 400)
    }
    catch {
        return $false
    }
}

if (Test-UrlReachable "http://localhost:8080") {
    Write-Host "Spark cluster is running (http://localhost:8080 is reachable)." -ForegroundColor Green
}
else {
    Write-Host "Spark cluster UI is not reachable yet at http://localhost:8080. Check docker logs for fi-spark." -ForegroundColor Red
}

if (-not (Test-UrlReachable "http://localhost:4040/jobs")) {
    Write-Host "http://localhost:4040/jobs is currently not reachable. This is expected until you run POST /spark/run (from dashboard button or API)." -ForegroundColor Yellow
}
