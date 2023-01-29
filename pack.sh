#!/bin/bash
set -e

[ -z $1 ] && echo "Missing version" && exit 1

version=$1
project=src/Enyim.Caching
dotnet clean -c Release
dotnet build -p:Version=${version-*} -c Release $project
dotnet pack $project -c Release -p:IncludeSymbols=true -p:SymbolPackageFormat=snupkg --include-source -p:PackageVersion=$version -p:Version=${version-*} -o ./artifacts
