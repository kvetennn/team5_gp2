from config.src.log_bootstrap import setup_logging, get_logger
def main():
    setup_logging()
    logger = get_logger("project.bootstrap")
    logger.info("Логирование настроено и работает.")
    logger.debug("Проверка DEBUG-сообщения (увидите его, если level=DEBUG).")
    logger.warning("Тестовое предупреждение - всё ок.")
    logger.error("Тестовая ошибка — демонстрация формата.", exc_info=False)

if __name__ == "__main__":
    main()

