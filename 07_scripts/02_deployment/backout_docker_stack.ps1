# Backout (rollback) the MedEnroll834 local Airflow Docker stack.
#
# Reverses `docker compose up -d` (docker-compose.yaml):
#   - stops and removes all medenroll834-* containers
#   - removes the airflow-logs and postgres-db-volume named volumes (if -Full)
#
# Usage:
#   .\07_scripts\02_deployment\backout_docker_stack.ps1          # stop + remove containers, keep volumes
#   .\07_scripts\02_deployment\backout_docker_stack.ps1 -Full    # also remove volumes (wipes Airflow metadata/logs)

param(
    [switch]$Full
)

Set-Location (Join-Path $PSScriptRoot "..\..")

Write-Output "Stopping and removing the Airflow docker-compose stack..."
if ($Full) {
    docker compose down -v
    Write-Output "Containers, networks, and volumes (airflow-logs, postgres-db-volume) removed."
} else {
    docker compose down
    Write-Output "Containers and networks removed. Volumes kept (re-run with -Full to also wipe them)."
}

Write-Output ""
Write-Output "Verifying no medenroll834-* containers remain..."
docker ps -a --filter "name=medenroll834" --format "{{.Names}}: {{.Status}}"

Write-Output ""
Write-Output "Backout complete. Re-run 'docker compose up -d' to redeploy the stack."
