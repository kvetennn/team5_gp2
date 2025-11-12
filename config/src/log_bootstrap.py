# src/log_bootstrap.py
import os
import logging
import logging.config
from logging import Logger
from pathlib import Path
import inspect
import yaml

FILE_TO_LOGGER = {
    "Author_Today.py":      "scraper.author_today",
    "litnet.py":            "scraper.litnet",
    "litnet_api.py":        "scraper.litnet_api",
    "litnet_unique_tags.py":"scraper.litnet_unique_tags",
    "litnet_xml.py":        "scraper.litnet_xml",
    "final.py":             "eda.final",
    "merge.ipynb":          "notebooks.merge",
}

DEFAULT_CONFIG_PATH = "config/logging.yaml"


def _apply_overrides(cfg: dict) -> dict:
    """
    Подменяем имя файла, размер ротации и кол-во бэкапов
    из верхних ключей YAML
    """
    log_dir = Path(cfg.get("log_dir", "logs"))
    log_dir.mkdir(parents=True, exist_ok=True)

    filename = cfg.get("filename", "app.log")
    full_path = log_dir / filename

    dict_cfg = cfg.get("dictConfig", {})
    handlers = dict_cfg.get("handlers", {})
    if "rotating_file" in handlers:
        h = handlers["rotating_file"]
        h["filename"] = str(full_path)
        h["maxBytes"] = int(cfg.get("rotate_megabytes", 10)) * 1024 * 1024
        h["backupCount"] = int(cfg.get("backup_count", 5))
        h["encoding"] = "utf-8"

    level = cfg.get("level")
    if level:
        dict_cfg.setdefault("root", {})
        dict_cfg["root"]["level"] = level

    cfg["dictConfig"] = dict_cfg
    return cfg


def _setup_from_yaml(config_path: str) -> None:
    """
    Инициализируем logging из YAML. При enabled=false логи выключены.
    При отсутствии файла включает базовую консольную конфигурацию.
    """
    if not os.path.exists(config_path):
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
        )
        logging.getLogger(__name__).warning(
            "Файл %s не найден. Включена базовая консольная конфигурация логирования",
            config_path,
        )
        return

    with open(config_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    if not cfg or not isinstance(cfg, dict):
        logging.basicConfig(level=logging.INFO)
        logging.getLogger(__name__).warning(
            "Невалидный YAML конфиг логирования, включена базовая конфигурация"
        )
        return

    enabled = bool(cfg.get("enabled", True))
    if not enabled:
        logging.disable(logging.CRITICAL)  # глушим всё
        return

    cfg = _apply_overrides(cfg)
    dict_cfg = cfg.get("dictConfig", {})
    logging.config.dictConfig(dict_cfg)


def setup_logging(config_path: str = DEFAULT_CONFIG_PATH) -> None:
    """Публичная функция"""
    _setup_from_yaml(config_path)


def get_logger(name: str | None = None) -> Logger:
    """
    Унифицированный способ получить логгер.
    """
    return logging.getLogger(name)


def auto_logger() -> Logger:
    """
    Возвращает логгер по имени файла, из которого вызвали функцию.
    Если соответствия нет, вернётся root-логгер.
    """
    # стек: [auto_logger, код], берём кадр вызывающего модуля
    frame = inspect.stack()[1]
    caller_file = os.path.basename(frame.filename)
    logger_name = FILE_TO_LOGGER.get(caller_file)
    return logging.getLogger(logger_name)


try:
    _setup_from_yaml(DEFAULT_CONFIG_PATH)
except Exception as e:
    # Ничего не ломаем, но даём понять в консоль
    logging.basicConfig(level=logging.INFO)
    logging.getLogger(__name__).error("Ошибка настройки логирования: %s", e)
