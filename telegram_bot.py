import os
import logging
import asyncio
from dotenv import load_dotenv
from datetime import datetime, timedelta, date
import re

# Импортируем нашу синхронную функцию из файла dashboard_generator.py
from dashboard_generator import generate_dashboard_from_text

# Импорты aiogram
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import CommandStart
from aiogram.types import Message, FSInputFile
from aiogram.client.default import DefaultBotProperties

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
    # Паттерн для поиска файлов: 'dashboard_data_X_NAME_YYYY-MM-DD.html'
    DASHBOARD_FILE_PATTERN = re.compile(r'dashboard_data_\d_[a-z]+_(\d{4}-\d{2}-\d{2})\.html$')

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
    # Так как command_start_handler определен выше, ссылка теперь работает.
    dp.message.register(command_start_handler, CommandStart())
    dp.message.register(handle_report_text, F.text)  # Обрабатываем любой текст

    logger.info("Бот aiogram запущен.")
    # Запускаем обработку входящих обновлений
    await dp.start_polling(bot)


if __name__ == '__main__':
    # Запуск асинхронной функции main()
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Бот остановлен пользователем.")
    except Exception as e:
        logger.error(f"Непредвиденная ошибка в основном цикле: {e}")
