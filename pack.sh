VERSION=$(git tag --sort=committerdate | tail -1)
dotnet build /p:version=$VERSION -c Release Enyim.Caching
dotnet pack -c Release /p:version=$VERSION Enyim.Caching

