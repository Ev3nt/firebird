
@echo off

:: Reset or clear some variables, as appropriate.
set ERRLEV=0
set FB_NOCLEAN=
set FBBUILD_BUILDTYPE=release
set FBBUILD_INCLUDE_PDB=
set FBBUILD_MAKE_KITS_ONLY=
set FBBUILD_BUILD_ONLY=0
set FBBUILD_KITS=ISX ZIP
set FBBUILD_TEST_ONLY=

::Check if on-line help is required
for %%v in ( %1 %2 %3 %4 %5 %6 %7 %8 %9 )  do (
  ( @if /I "%%v"=="-h" (goto :HELP & goto :EOF) )
  ( @if /I "%%v"=="/h" (goto :HELP & goto :EOF) )
  ( @if /I "%%v"=="HELP" (goto :HELP & goto :EOF) )
)


:: Read the command line
for %%v in ( %* )  do (
( if /I "%%v"=="NOCLEAN" (set FB_NOCLEAN=1) )
( if /I "%%v"=="DEBUG" (set FBBUILD_BUILDTYPE=debug) )
( if /I "%%v"=="PDB" (set FBBUILD_INCLUDE_PDB=1) )
( if /I "%%v"=="REPACK" (set FBBUILD_MAKE_KITS_ONLY=1) )
( if /I "%%v"=="JUSTBUILD" (set FBBUILD_BUILD_ONLY=1) )
( if /I "%%v"=="TESTENV" (set FBBUILD_TEST_ONLY=1) )
( if /I "%%v"=="NO_RN" set FB_EXTERNAL_DOCS=)
( if /I "%%v"=="NO_RN" set FBBUILD_PACKAGE_NUMBER=)
)

if defined FBBUILD_TEST_ONLY ( goto TEST_ENV & goto :EOF )

if defined FBBUILD_MAKE_KITS_ONLY (goto :MAKE_KITS & goto :EOF)

:: Go to work
if not defined FB_NOCLEAN (call clean_all)
:: We do not support debug builds of icu, so we don't pass %FBBUILD_BUILDTYPE%
call make_icu
if "%ERRLEV%"=="1" goto :END
call make_boot %FBBUILD_BUILDTYPE%
if "%ERRLEV%"=="1" goto :END
call make_all %FBBUILD_BUILDTYPE%
if "%ERRLEV%"=="1" goto :END
call make_examples %FBBUILD_BUILDTYPE%
if "%ERRLEV%"=="1" goto :END

if "%FBBUILD_BUILD_ONLY%"=="1" goto :END

:MAKE_KITS
:: Package everything up
pushd ..\install\arch-specific\win32
call BuildExecutableInstall %FBBUILD_KITS% %FBBUILD_BUILDTYPE%
if "%ERRLEV%"=="1" (
  @echo Oops - some sort of error during packaging & popd & goto :END
)
if defined FBBUILD_INCLUDE_PDB (
  set /A FBBUILD_PACKAGE_NUMBER-=1
  call BuildExecutableInstall %FBBUILD_KITS% %FBBUILD_BUILDTYPE% PDB
)
popd

goto :END
::---------

:HELP
@echo.
@echo The following params may be passed:
@echo.
@echo    NOCLEAN   - don't run CLEAN_ALL.BAT
@echo.
@echo    DEBUG     - Do a DEBUG build (for experienced developers only.)
@echo                This switch is not needed to debug Firebird.
@echo.
@echo    PDB       - Create PDB packages as well as standard kits
@echo.
@echo    REPACK    - Don't build - just repack kits.
@echo.
@echo    JUSTBUILD - Just build - don't create packages.
@echo.
@echo    TESTENV   - Sanity check - is Visual Studio available?.
@echo                             - print the build variables that will be used
@echo.
@echo    NO_RN     - Do not fail the packaging if release notes unavailable.
@echo                Default is to fail if FB_EXTERNAL_DOCS is set and release notes not found.
@echo.
@goto :EOF


:TEST_ENV
::===============================
:: Show variables
@call setenvvar.bat %*
if "%ERRLEV%"=="1" goto :END
for /F "tokens=2*" %%a in ( %FB_ROOT_PATH%\gen\jrd\build_no.h ) do ( SET %%a=%%~b )
set > %FB_ROOT_PATH%\builds\install\arch-specific\win32\test_installer\fb_build_vars_%PROCESSOR_ARCHITECTURE%.txt
type  %FB_ROOT_PATH%\builds\install\arch-specific\win32\test_installer\fb_build_vars_%PROCESSOR_ARCHITECTURE%.txt
goto :END
::---------


:END

if "%ERRLEV%"=="1" exit /b 1
