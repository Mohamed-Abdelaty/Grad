import os
from app.core.config import Settings

def test_compose_project_name_is_consistent():
    os.environ["COMPOSE_PROJECT_NAME"] = "my-custom-project"
    settings = Settings()
    assert settings.PROJECT_CONTAINER_PREFIX == "my-custom-project"
    
    os.environ.pop("COMPOSE_PROJECT_NAME")
    settings_default = Settings()
    assert settings_default.PROJECT_CONTAINER_PREFIX == "resilient-big-data-platform"
