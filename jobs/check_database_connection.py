"""
Validate database connectivity using DATABASE_URL/OLJ_DB_PATH configuration.
"""

import logging

from dotenv import load_dotenv

from services.database import DatabaseConfigError, build_connection_factory_from_env

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)


if __name__ == "__main__":
    load_dotenv()

    factory = build_connection_factory_from_env(require_config=True)
    try:
        factory.healthcheck()
    except NotImplementedError as exc:
        raise RuntimeError(
            "Database dialect is configured but runtime connector is not implemented in this repo yet.",
        ) from exc
    except DatabaseConfigError:
        raise
    except Exception as exc:
        raise RuntimeError("Database connectivity check failed.") from exc

    logger.info("Database connectivity check passed (dialect=%s).", factory.dialect.value)
