@echo off
REM Snowflake to Kafka CDC - Enterprise Build Script (Windows)
REM This script handles compilation, testing, packaging, and deployment

setlocal enabledelayedexpansion

REM Configuration
set PROJECT_NAME=snowflake-kafka-cdc
set DOCKER_TAG=latest

REM Get version from Maven
for /f "delims=" %%i in ('mvn help:evaluate -Dexpression=project.version -q -DforceStdout') do set VERSION=%%i

REM Colors (limited support in Windows)
set RED=[91m
set GREEN=[92m
set YELLOW=[93m
set BLUE=[94m
set NC=[0m

REM Functions
:print_header
echo %BLUE%================================================%NC%
echo %BLUE% %~1%NC%
echo %BLUE%================================================%NC%
goto :eof

:print_success
echo %GREEN%✓ %~1%NC%
goto :eof

:print_warning
echo %YELLOW%! %~1%NC%
goto :eof

:print_error
echo %RED%✗ %~1%NC%
goto :eof

:print_info
echo %BLUE%i %~1%NC%
goto :eof

REM Check prerequisites
:check_prerequisites
call :print_header "Checking Prerequisites"

REM Check Java
java -version >nul 2>&1
if errorlevel 1 (
    call :print_error "Java is not installed or not in PATH"
    exit /b 1
)

for /f "tokens=3" %%g in ('java -version 2^>^&1 ^| findstr /i "version"') do (
    set JAVA_VERSION=%%g
    set JAVA_VERSION=!JAVA_VERSION:"=!
)
call :print_info "Java version: !JAVA_VERSION!"

REM Check Maven
mvn -version >nul 2>&1
if errorlevel 1 (
    call :print_error "Maven is not installed or not in PATH"
    exit /b 1
)

for /f "tokens=*" %%g in ('mvn -version ^| findstr "Apache Maven"') do (
    call :print_info "%%g"
)

REM Check Docker (optional)
docker --version >nul 2>&1
if errorlevel 1 (
    call :print_warning "Docker not found - Docker operations will be skipped"
) else (
    for /f "tokens=*" %%g in ('docker --version') do (
        call :print_info "%%g"
    )
)

call :print_success "Prerequisites check completed"
goto :eof

REM Clean build artifacts
:clean
call :print_header "Cleaning Build Artifacts"

mvn clean
if errorlevel 1 exit /b 1

REM Clean Docker images if requested
if "%~1"=="--docker" (
    docker --version >nul 2>&1
    if not errorlevel 1 (
        call :print_info "Cleaning Docker images..."
        docker rmi %PROJECT_NAME%:%DOCKER_TAG% 2>nul
        docker rmi %PROJECT_NAME%:%VERSION% 2>nul
    )
)

call :print_success "Clean completed"
goto :eof

REM Compile the project
:compile
call :print_header "Compiling Project"

mvn compile
if errorlevel 1 exit /b 1

call :print_success "Compilation completed"
goto :eof

REM Run tests
:test
call :print_header "Running Tests"

call :print_info "Running unit tests..."
mvn test
if errorlevel 1 exit /b 1

call :print_success "All tests passed"
goto :eof

REM Package the application
:package
call :print_header "Packaging Application"

mvn package -DskipTests
if errorlevel 1 exit /b 1

REM Create distribution directory
if not exist dist mkdir dist

REM Copy JAR file
copy target\%PROJECT_NAME%-%VERSION%.jar dist\ >nul

REM Copy configuration files
copy src\main\resources\application.conf dist\ >nul
copy env.example dist\ >nul

REM Copy documentation
copy README.md dist\ >nul
xcopy docs dist\docs\ /E /I /Q >nul

REM Create startup script
(
echo @echo off
echo REM Startup script for Snowflake Kafka CDC
echo.
echo REM Set JVM options for production
echo set JAVA_OPTS=%%JAVA_OPTS%% -Xms2g -Xmx4g
echo set JAVA_OPTS=%%JAVA_OPTS%% -XX:+UseG1GC
echo set JAVA_OPTS=%%JAVA_OPTS%% -XX:MaxGCPauseMillis=100
echo set JAVA_OPTS=%%JAVA_OPTS%% -XX:+UseStringDeduplication
echo set JAVA_OPTS=%%JAVA_OPTS%% -XX:+ExitOnOutOfMemoryError
echo set JAVA_OPTS=%%JAVA_OPTS%% -XX:+HeapDumpOnOutOfMemoryError
echo set JAVA_OPTS=%%JAVA_OPTS%% -XX:HeapDumpPath=.\logs\
echo set JAVA_OPTS=%%JAVA_OPTS%% -Djava.security.egd=file:/dev/./urandom
echo.
echo REM Enable JMX monitoring
echo set JAVA_OPTS=%%JAVA_OPTS%% -Dcom.sun.management.jmxremote
echo set JAVA_OPTS=%%JAVA_OPTS%% -Dcom.sun.management.jmxremote.port=9999
echo set JAVA_OPTS=%%JAVA_OPTS%% -Dcom.sun.management.jmxremote.authenticate=false
echo set JAVA_OPTS=%%JAVA_OPTS%% -Dcom.sun.management.jmxremote.ssl=false
echo.
echo REM Create logs directory
echo if not exist logs mkdir logs
echo.
echo REM Start application
echo java %%JAVA_OPTS%% -jar snowflake-kafka-cdc-*.jar
) > dist\start.bat

REM Create archive (using PowerShell)
powershell -command "Compress-Archive -Path 'dist\*' -DestinationPath '%PROJECT_NAME%-%VERSION%.zip' -Force"

call :print_success "Packaging completed - Archive: %PROJECT_NAME%-%VERSION%.zip"
goto :eof

REM Build Docker image
:docker_build
docker --version >nul 2>&1
if errorlevel 1 (
    call :print_warning "Docker not available - skipping Docker build"
    goto :eof
)

call :print_header "Building Docker Image"

docker build -t %PROJECT_NAME%:%VERSION% .
if errorlevel 1 exit /b 1

docker tag %PROJECT_NAME%:%VERSION% %PROJECT_NAME%:%DOCKER_TAG%

REM Tag for registry if specified
if not "%DOCKER_REGISTRY%"=="" (
    docker tag %PROJECT_NAME%:%VERSION% %DOCKER_REGISTRY%/%PROJECT_NAME%:%VERSION%
    docker tag %PROJECT_NAME%:%VERSION% %DOCKER_REGISTRY%/%PROJECT_NAME%:%DOCKER_TAG%
)

call :print_success "Docker image built: %PROJECT_NAME%:%VERSION%"
goto :eof

REM Generate reports
:reports
call :print_header "Generating Reports"

if not exist reports mkdir reports

REM Test reports
if exist target\surefire-reports (
    xcopy target\surefire-reports reports\surefire-reports\ /E /I /Q >nul
    call :print_info "Test reports copied to reports\surefire-reports"
)

REM Dependency reports
mvn dependency:tree > reports\dependency-tree.txt
mvn dependency:analyze > reports\dependency-analysis.txt

REM Project info
(
echo Build Information
echo =================
echo Project: %PROJECT_NAME%
echo Version: %VERSION%
echo Build Date: %DATE% %TIME%
for /f "tokens=3" %%g in ('java -version 2^>^&1 ^| findstr /i "version"') do echo Java Version: %%g
for /f "tokens=*" %%g in ('mvn -version ^| findstr "Apache Maven"') do echo Maven Version: %%g
) > reports\build-info.txt

call :print_success "Reports generated in reports\ directory"
goto :eof

REM Show help
:show_help
echo Snowflake to Kafka CDC - Enterprise Build Script (Windows)
echo.
echo Usage: %0 [COMMAND] [OPTIONS]
echo.
echo Commands:
echo   check         Check prerequisites
echo   clean         Clean build artifacts (use --docker to also clean Docker images)
echo   compile       Compile the project
echo   test          Run all tests
echo   package       Package the application
echo   docker-build  Build Docker image
echo   reports       Generate build reports
echo   all           Run complete build pipeline
echo   help          Show this help message
echo.
echo Environment Variables:
echo   DOCKER_REGISTRY   Docker registry URL (optional)
echo   DOCKER_TAG        Docker tag (default: latest)
echo.
echo Examples:
echo   %0 all                    # Complete build pipeline
echo   %0 clean --docker         # Clean including Docker images
echo   set DOCKER_REGISTRY=my.registry.com ^& %0 docker-build
goto :eof

REM Main execution
if "%1"=="" goto show_help
if "%1"=="help" goto show_help
if "%1"=="--help" goto show_help
if "%1"=="-h" goto show_help

if "%1"=="check" (
    call :check_prerequisites
) else if "%1"=="clean" (
    call :clean %2
) else if "%1"=="compile" (
    call :compile
) else if "%1"=="test" (
    call :test
) else if "%1"=="package" (
    call :package
) else if "%1"=="docker-build" (
    call :docker_build
) else if "%1"=="reports" (
    call :reports
) else if "%1"=="all" (
    call :check_prerequisites
    if errorlevel 1 exit /b 1
    call :clean
    if errorlevel 1 exit /b 1
    call :compile
    if errorlevel 1 exit /b 1
    call :test
    if errorlevel 1 exit /b 1
    call :package
    if errorlevel 1 exit /b 1
    call :docker_build
    if errorlevel 1 exit /b 1
    call :reports
    if errorlevel 1 exit /b 1
    call :print_header "Build Pipeline Completed Successfully"
    call :print_success "All stages completed successfully!"
    call :print_info "Artifacts:"
    call :print_info "  - JAR: target\%PROJECT_NAME%-%VERSION%.jar"
    call :print_info "  - Archive: %PROJECT_NAME%-%VERSION%.zip"
    call :print_info "  - Docker: %PROJECT_NAME%:%VERSION%"
    call :print_info "  - Reports: reports\"
) else (
    call :print_error "Unknown command: %1"
    echo.
    goto show_help
)

endlocal
