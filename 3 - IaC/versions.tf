// Version & provider constraints.
// Usar operadores pesimistas (~>) fija mayor estabilidad evitando upgrades mayores inesperados.
// Ajusta las versiones a lo que ya has probado en tu entorno.
terraform {
  required_version = ">= 1.6.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      // Bloqueo actual en .terraform.lock.hcl: 6.8.0
      // Usamos ~> 6.8 para permitir parches (<6.9.0) sin saltar minor.
      version = "~> 6.8"
    }
    random = {
      source  = "hashicorp/random"
      // Bloqueo actual en .terraform.lock.hcl: 3.7.2
      version = "~> 3.7"
    }
  }
}

// Tras actualizar constraints, ejecuta:
//   terraform init -upgrade
// Para forzar un downgrade (ej. volver a aws 5.x):
//   1. Cambia version = "~> 5.0"
//   2. Elimina .terraform.lock.hcl
//   3. terraform init