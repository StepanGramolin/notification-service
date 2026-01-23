# NotificationService Dockerfile
# Build
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

COPY . .
RUN dotnet restore ./NotificationService/NotificationService.csproj
RUN dotnet publish ./NotificationService/NotificationService.csproj -c Release -o /app/publish /p:UseAppHost=false

# Run
FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS final
WORKDIR /app
EXPOSE 8080
COPY --from=build /app/publish .
ENV ASPNETCORE_URLS=http://+:8080
ENTRYPOINT ["dotnet", "NotificationService.dll"]