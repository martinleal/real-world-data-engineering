## deploy_checks.py

Script para desplegar secuencialmente ficheros de check rules usando un único pipeline de Azure DevOps.

### Requisitos entorno
Define estas variables de entorno (PowerShell):
```powershell
$env:AZDO_ORG_URL = "YOUR_ORG_URL"
$env:AZDO_PROJECT = "YOUR_PROJECT_NAME"
$env:AZDO_PAT = "PERSONAL_ACCESS_TOKEN"
```

### Fichero de configuración
Ejemplo `checks_to_deploy.json`:
```json
{
  "pipeline": 164,
  "branch": "develop",
  "env": "PRE",
  "checks": [
    "/config/config_checks/DIM_PLAN_ACTION_EVIDENCE_STATUS/CONSISTENCY__SOSTENIBILIDAD__DHWSOSTENIBILIDAD_DATA__DIM_PLAN_ACTION_EVIDENCE_STATUS.yaml",
    "/config/config_checks/DIM_PLAN_ACTION_STAGE/UNIQUENESS__SOSTENIBILIDAD__DHWSOSTENIBILIDAD_DATA__DIM_PLAN_ACTION_STAGE.yaml"
  ]
}
```

Coloca el script y el JSON en la carpeta `deploy checks/`. Ten en cuenta el espacio en el nombre; si ejecutas desde otra ruta puedes usar comillas.

### Ejecución básica
```powershell
python .\deploy_checks.py --config .\checks_to_deploy.json
```

### Opciones
- `--debug` Muestra el payload enviado al API.
- `--json` Salida final en JSON (para parsear en otros procesos).
- `--no-wait` Dispara cada run sin esperar su resultado (pero sigue siendo secuencial en el disparo).
- `--interval` Segundos entre polls (por defecto 15).
- `--check-folder` Cambiar la carpeta de consultas (`/config/check_queries`).

### Resultado
Al terminar muestra un resumen por cada fichero indicando `succeeded`, `failed` o `error`. Código de salida 0 si todos succeeded, 1 si alguno falla.

### Buenas prácticas
- Usa `--debug` la primera vez para verificar los flags forzados: `doDeploySnowflakeCICD=False`, `doDeployDG=False`, `doDeployAlerts=False`, `doDeployCheckRules=True`.
