import gzip
import logging
import os
from datetime import datetime
from pathlib import Path

import docker

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


def ensure_backup_dir(backup_dir):
    """Create backup directory if it doesn't exist."""
    Path(backup_dir).mkdir(parents=True, exist_ok=True)
    return backup_dir


def safe_filename(name):
    """Create a safe filename from potentially unsafe string."""
    return "".join(c for c in name if c.isalnum() or c in ('_', '-')).rstrip()


def backup_mysql(container, backup_dir):
    """Perform MySQL backup with improved security and error handling."""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_file = os.path.join(backup_dir, f"mysql_backup_{safe_filename(container.name)}_{timestamp}.sql.gz")

    container_env = container.attrs['Config']['Env']
    env_dict = dict(item.split('=', 1) for item in container_env)

    mysql_user = env_dict.get('MYSQL_USER', 'root')
    mysql_password = env_dict.get('MYSQL_PASSWORD', '')
    mysql_database = env_dict.get('MYSQL_DATABASE', '')

    # Use a more secure way to pass password
    cmd = f'mysqldump -u {mysql_user} --databases {mysql_database}'
    environment = {"MYSQL_PWD": mysql_password}

    exit_code, output = container.exec_run(cmd, environment=environment)

    if exit_code == 0:
        with gzip.open(backup_file, 'wb') as f:
            f.write(output)
        logger.info(f"MySQL backup saved to {backup_file}")
        return True
    else:
        logger.error(f"MySQL backup failed for {container.name}: {output.decode()}")
        return False


def backup_django(container, backup_dir):
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_file = os.path.join(backup_dir,
                               f"django_backup_{safe_filename(container.name)}_{timestamp}.json.gz")

    exit_code, output = container.exec_run(
        "python3 manage.py dumpdata -e contenttypes -e auth.permission --natural-foreign --natural-primary"
    )

    if exit_code == 0:
        with gzip.open(backup_file, 'wb') as f:
            f.write(output)
        logger.info(f"Django backup saved to {backup_file}")
        return True
    else:
        logger.error(f"Django backup failed for {container.name}: {output.decode()}")
        return False


def backup_volume(container, vol, backup_dir):
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_file = os.path.join(backup_dir,
                               f"volume_backup_{safe_filename(container.name)}_{safe_filename(vol)}_{timestamp}.tar.gz")

    try:
        bits, stat = container.get_archive(vol)
        with gzip.open(backup_file, 'wb') as gz_file:
            for chunk in bits:
                gz_file.write(chunk)
        logger.info(f"Volume backup saved to {backup_file}")
        return True
    except Exception as e:
        logger.error(f"Error backing up volume {vol} for container {container.name}: {e}")
        return False


def main():
    backup_dir = "/backup" if os.path.exists("/backup") else "./backup"
    ensure_backup_dir(backup_dir)

    try:
        client = docker.from_env()
        containers = client.containers.list()
    except docker.errors.DockerException as e:
        logger.error(f"Failed to connect to Docker: {e}")
        return

    for container in containers:
        labels = container.labels

        if labels.get("DCBAK", "").lower() != "true":
            continue

        logger.info(f"Backing up {container.name}")

        try:
            backup_type = labels.get("DCBAK-TYPE", "")

            if backup_type == "mysql":
                logger.info(" > MySQL Backup")
                backup_mysql(container, backup_dir)

            elif backup_type == "django":
                logger.info(" > Django Backup")
                backup_django(container, backup_dir)

            if labels.get("DCBAK-VOLUME", False):
                logger.info(" > Volume Backup")
                vol = labels.get("DCBAK-VOLUME")

                backup_volume(container, vol, backup_dir)

        except Exception as e:
            logger.error(f"Error processing container {container.name}: {e}")


if __name__ == "__main__":
    main()
