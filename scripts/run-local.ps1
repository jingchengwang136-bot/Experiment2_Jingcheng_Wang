param(
    [Parameter(Mandatory=$true)][string]$Train,
    [Parameter(Mandatory=$true)][string]$Test,
    [Parameter(Mandatory=$true)][string]$Output,
    [Parameter(Mandatory=$true)][string]$HadoopHome,
    [switch]$WindowsCompatibility
)
$ErrorActionPreference = 'Stop'
# Java 8 is recommended for this Windows local execution setup.
# Execute from the project root. Hadoop native helpers are not bundled.
$env:HADOOP_HOME = (Resolve-Path -LiteralPath $HadoopHome).Path
$env:PATH = "$env:HADOOP_HOME\bin;$env:PATH"
& mvn -q package dependency:build-classpath '-Dmdep.outputFile=target/classpath.txt'
if ($LASTEXITCODE -ne 0) { throw 'Maven build failed' }
$cp = (Get-Content -LiteralPath target/classpath.txt -Raw).Trim()
$tempRoot = Join-Path (Get-Location) '.local-runtime'
New-Item -ItemType Directory -Force -Path $tempRoot | Out-Null
$runtimeClasspath = "target/credit-mapreduce.jar;$cp"
$options = @('-Dmapreduce.framework.name=local', '-Dfs.defaultFS=file:///', "-Dhadoop.tmp.dir=$tempRoot", "-Dmapreduce.jobtracker.staging.root.dir=$tempRoot/staging")
if ($WindowsCompatibility) {
    $classes = Join-Path $tempRoot 'compat-classes'
    New-Item -ItemType Directory -Force -Path $classes | Out-Null
    $sources = @(Get-ChildItem -LiteralPath scripts/windows-compat -Recurse -Filter '*.java' | ForEach-Object { $_.FullName })
    & javac -source 8 -target 8 -encoding UTF-8 -cp $cp -d $classes @sources
    if ($LASTEXITCODE -ne 0) { throw 'Compatibility adapter compilation failed' }
    $runtimeClasspath = "$classes;$runtimeClasspath"
    $options += '-Dfs.file.impl=edu.lab.WindowsLocalFileSystem'
}
& java "-Djava.library.path=$env:HADOOP_HOME\bin" "-Dhadoop.home.dir=$env:HADOOP_HOME" "-Djava.io.tmpdir=$tempRoot" -Xmx2g -cp $runtimeClasspath edu.lab.CreditAnalysis @options $Train $Test $Output
if ($LASTEXITCODE -ne 0) { throw 'Hadoop pipeline failed' }
