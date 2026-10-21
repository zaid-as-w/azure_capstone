# Automated Deployment Script for Azure Data Factory Pipelines
# Usage: .\scripts\deploy_adf_pipelines.ps1 -ResourceGroup "my-group" -FactoryName "zaid-capstone-adf"

param(
    [string]$ResourceGroup = "my-group",
    [string]$FactoryName = "zaid-capstone-adf"
)

$ErrorActionPreference = "Stop"

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "Deploying Pipelines to Azure Data Factory: $FactoryName" -ForegroundColor Cyan
Write-Host "Resource Group: $ResourceGroup" -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

$pipelines = @(
    @{ Name = "PL_Sales_Bronze_To_Silver"; File = "adf/pipeline_bronze_to_silver.json" },
    @{ Name = "PL_Sales_Bronze_To_Silver_To_Gold"; File = "adf/pipeline_bronze_to_silver_to_gold.json" },
    @{ Name = "PL_Sales_Incremental_Ingestion"; File = "adf/pipeline_incremental_ingestion.json" }
)

foreach ($pl in $pipelines) {
    Write-Host "Deploying pipeline '$($pl.Name)' from $($pl.File)..." -ForegroundColor Yellow
    az datafactory pipeline create `
        --factory-name $FactoryName `
        --resource-group $ResourceGroup `
        --name $pl.Name `
        --pipeline "@$($pl.File)" `
        --output none
    Write-Host "  -> Successfully deployed $($pl.Name)" -ForegroundColor Green
}

Write-Host "`nAll pipelines deployed successfully!" -ForegroundColor Green
az datafactory pipeline list --factory-name $FactoryName --resource-group $ResourceGroup --query "[].{name:name, activities:length(activities)}" --output table
