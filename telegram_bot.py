import os
import logging
import asyncio
from dotenv import load_dotenv
from datetime import datetime, timedelta, date
import re
import requests  # НОВЫЙ ИМПОРТ

# Импортируем нашу синхронную функцию из файла dashboard_generator.py
from dashboard_generator import generate_dashboard_from_text, download_and_process_google_sheet, \
    generate_slideshow_host, LATEST_DASHBOARD_FILE, upload_files_to_sftp, DASHBOARD_PREFIX_GS, NEW_FILES_LIST

# Импорты aiogram
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import CommandStart
from aiogram.types import Message, FSInputFile
from aiogram.client.default import DefaultBotProperties

# НОВЫЕ ИМПОРТЫ ДЛЯ ПЛАНИРОВАНИЯ
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# --- КОНСТАНТЫ И ИНИЦИАЛИЗАЦИЯ ---

# Загружаем переменные окружения из файла .env
load_dotenv()
BOT_TOKEN = os.getenv('BOT_TOKEN')

# Настройка логгирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# --- ВСПОМОГАТЕЛЬНАЯ ФУНКЦИЯ ДЛЯ УДАЛЕНИЯ СТАРЫХ ФАЙЛОВ ---

def cleanup_old_dashboards(days_to_keep: int = 1):
    """
    Удаляет старые файлы дашбордов, оставляя только те, что были созданы
    в течение последних N дней.
    """
    # Паттерн для поиска файлов: 'dashboard_data_X_NAME_YYYY-MM-DD.html' И НОВЫЙ ПРЕФИКС GS
    DASHBOARD_FILE_PATTERN = re.compile(r'(?:dashboard_data|dashboard_gs_data)_\d_[a-z]+_(\d{4}-\d{2}-\d{2}).*\.html$')

    # Дата-порог, все, что старше, будет удалено
    cutoff_date = date.today() - timedelta(days=days_to_keep)

    deleted_count = 0

    logger.info(f"Начинаю очистку старых дашбордов. Удаляются файлы старше {days_to_keep} дней (до {cutoff_date}).")

    # Перебираем все файлы в текущей директории
    for filename in os.listdir('.'):
        match = DASHBOARD_FILE_PATTERN.match(filename)

        # Если файл соответствует паттерну
        if match:
            file_date_str = match.group(1)

            try:
                file_date = datetime.strptime(file_date_str, '%Y-%m-%d').date()

                # Сравниваем дату файла с пороговой датой
                if file_date < cutoff_date:
                    os.remove(filename)
                    deleted_count += 1
                    logger.info(f"🗑️ Удален старый файл: {filename}")

            except ValueError:
                # Если дату не удалось распарсить, пропускаем файл
                logger.warning(f"Не удалось распарсить дату из имени файла: {filename}")
                continue

    logger.info(f"✅ Очистка завершена. Удалено {deleted_count} файлов.")
    return deleted_count


# --- ФУНКЦИИ ПЛАНИРОВЩИКА ---

async def scheduled_dashboard_update():
    """
    Ежечасное обновление графиков Google Sheet и перезапуск слайдшоу.
    """
    print("⏰ Запущено ежечасное обновление графиков Google Sheet...")
    try:
        # 1. Генерация новых графиков (сохраняет пути в NEW_FILES_LIST)
        new_gs_files = download_and_process_google_sheet()

        if new_gs_files:
            # 2. Генерация главного файла слайдшоу.
            # Мы передаем пустой список для ботовых файлов, так как они не хранят историю
            # в глобальном состоянии. generate_slideshow_host объединит их с NEW_FILES_LIST.
            slideshow_host_file = generate_slideshow_host([], date.today())

            # 3. Загрузка только НОВЫХ файлов и хоста
            load_dotenv()
            remote_path = os.getenv('SFTP_PATH', '/')
            all_files_to_upload = new_gs_files + [slideshow_host_file]
            upload_files_to_sftp(all_files_to_upload, remote_path)

            logger.info(f"✅ Ежечасное обновление завершено. Обновлено {len(new_gs_files)} графиков и хост.")

        else:
            logger.warning("⚠️ Графики Google Sheets не сгенерированы (проблема с данными или загрузкой).")

    except Exception as e:
        logger.error(f"Критическая ошибка в ежечасном обновлении: {e}", exc_info=True)


# --- ОБРАБОТЧИКИ ---

async def command_start_handler(message: Message) -> None:
    """Обрабатывает команду /start."""
    await message.answer(
        'Привет! Я Генератор Дэшбордов ОКК.\n\n'
        'Просто **отправьте мне текст ежедневного отчета**, и я сгенерирую '
        'интерактивный HTML-дашборд и отправлю его вам.',
        parse_mode='Markdown'
    )


async def handle_report_text(message: Message) -> None:
    """
    Обрабатывает текстовые сообщения (предполагая, что это текст отчета).
    """
    report_text = message.text

    # 1. Уведомляем пользователя о начале обработки
    processing_message = await message.answer(
        '🚀 Начинаю анализ отчета и генерацию дашборда... Пожалуйста, подождите.'
    )

    html_file_path = None
    try:
        # --- ШАГ 1.5: ОЧИСТКА СТАРЫХ ФАЙЛОВ (запуск в потоке) ---
        # Запускаем синхронную функцию очистки в отдельном потоке
        await asyncio.to_thread(cleanup_old_dashboards, days_to_keep=7)

        # 2. Вызываем синхронную функцию генерации дашборда
        html_file_path = await asyncio.to_thread(
            generate_dashboard_from_text,
            report_text
        )

        if html_file_path:
            # 3. Отправляем готовый HTML-файл
            file_to_send = FSInputFile(html_file_path, filename=os.path.basename(html_file_path))

            await message.answer_document(
                document=file_to_send,
                caption='✅ Ваш интерактивный дашборд готов!'
            )
            logger.info(f"Дашборд {html_file_path} успешно отправлен.")
        else:
            await message.answer(
                '⚠️ Не удалось сгенерировать дашборд. Недостаточно данных в отчете.'
            )

    except ValueError as e:
        # Обработка ошибок парсинга
        error_msg = str(e).replace("Ошибка парсинга отчета. Проверьте формат. Детали: ",
                                   "❌ **Ошибка формата отчета!**\n")
        await message.answer(f'{error_msg}', parse_mode='Markdown')
    except Exception as e:
        # Обработка других критических ошибок
        logger.error(f"Критическая ошибка при обработке: {e}", exc_info=True)
        await message.answer(
            '❌ Произошла критическая ошибка при обработке отчета. '
            'Проверьте логи сервера.'
        )
    finally:
        # 4. Удаляем промежуточное сообщение и временный файл для отправки
        await processing_message.delete()
        if html_file_path and os.path.exists(html_file_path):
            os.remove(html_file_path)
            logger.info(f"Временный файл {html_file_path} удален.")


# --- MAIN ФУНКЦИЯ ЗАПУСКА ---

async def main() -> None:
    """Инициализация и запуск бота."""

    if not BOT_TOKEN:
        logger.error("❌ Критическая ошибка: Токен бота не найден. Убедитесь, что BOT_TOKEN установлен в файле .env.")
        return

    # Используем DefaultBotProperties для установки параметров по умолчанию
    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode='HTML'))
    dp = Dispatcher()

    # Регистрируем обработчики.
    dp.message.register(command_start_handler, CommandStart())
    dp.message.register(handle_report_text, F.text)  # Обрабатываем любой текст

    # 1. Инициализация планировщика
    scheduler = AsyncIOScheduler()

    # 2. Добавление задачи: запуск каждый час
    # ИСПРАВЛЕНИЕ: Используем datetime.now() + timedelta(seconds=60)
    first_run_time = datetime.now() + timedelta(seconds=3)

    scheduler.add_job(scheduled_dashboard_update, 'interval', hours=1,
                      next_run_time=first_run_time)

    # 3. Запуск планировщика
    scheduler.start()

    logger.info("Бот aiogram запущен. Планировщик запущен.")

    # 4. Запуск бота (существующий код)
    try:
        await dp.start_polling(bot)
    finally:
        await bot.session.close()


if __name__ == '__main__':
    # Запуск асинхронной функции main()
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Бот остановлен пользователем.")
    except Exception as e:
        logger.error(f"Непредвиденная ошибка в основном цикле: {e}")
