import pandas as pd
import re
from datetime import datetime, date
import os
import plotly.express as px
import plotly.graph_objects as go
import paramiko  # Для SFTP-соединения
from dotenv import load_dotenv  # Для чтения переменных окружения из .env

# --- КОНСТАНТЫ И КОНФИГУРАЦИЯ ---
INPUT_REPORT_FILE = 'latest_report.txt'
STAFF_HISTORY_FILE = 'staff_report_history.csv'
METRICS_HISTORY_FILE = 'metrics_report_history.csv'
LATEST_DASHBOARD_FILE = 'latest_dashboard.html'
SLIDESHOW_INTERVAL_SECONDS = 15
DASHBOARD_PREFIX = 'dashboard_data'  # Префикс для файлов с данными

# --- КОНСТАНТЫ СТИЛИЗАЦИИ ---
COLOR_COMPLETED = 'rgb(136, 190, 67)'  # Выполнено (зеленый)
COLOR_MISSED = 'rgb(240, 102, 0)'  # Не выполнено / Просрочено (оранжевый)
PLOTLY_HEIGHT = 720  # Оптимизированная высота для 1366x768 (768 минус заголовок и отступы)

# --- НОВАЯ КОНСТАНТА ДЛЯ URL ФОНА ---
BACKGROUND_URL = 'https://disk.yandex.ru/i/wAjsKqMrRGPpkQ'


# --- ФУНКЦИИ SFTP ---

def upload_files_to_sftp(local_file_paths: list[str], remote_dir: str) -> bool:
    """Загружает список файлов на удаленный SFTP-сервер."""

    load_dotenv()

    SFTP_HOST = os.getenv('SFTP_HOST')
    SFTP_USER = os.getenv('SFTP_USER')
    SFTP_PASS = os.getenv('SFTP_PASS')

    if not all([SFTP_HOST, SFTP_USER, SFTP_PASS]):
        print("⚠️ Не удалось загрузить файлы: Отсутствуют SFTP-параметры в .env (SFTP_HOST, SFTP_USER, SFTP_PASS).")
        return False

    try:
        transport = paramiko.Transport((SFTP_HOST, 22))
        transport.connect(username=SFTP_USER, password=SFTP_PASS)
        sftp = paramiko.SFTPClient.from_transport(transport)

        # 1. Загрузка всех файлов
        for local_path in local_file_paths:
            remote_path = os.path.join(remote_dir, os.path.basename(local_path))
            # Примечание: фон.jpg (или сам файл фона) больше не загружается, так как используется URL.
            sftp.put(local_path, remote_path)
            print(f"⬆️ Успешно загружен {os.path.basename(local_path)} на {SFTP_HOST}")

        sftp.close()
        transport.close()
        return True

    except Exception as e:
        print(f"❌ Критическая ошибка SFTP-загрузки: {e}")
        return False


# --- ФУНКЦИИ РАБОТЫ С ДАННЫМИ ---

def load_data_from_file(file_path: str) -> pd.DataFrame:
    """
    Загружает исторические данные из CSV-файла.
    """
    if os.path.exists(file_path):
        df = pd.read_csv(file_path)
        # Преобразуем в datetime.date для единообразия
        df['Дата'] = pd.to_datetime(df['Дата'], format='mixed').dt.date
        return df
    return pd.DataFrame()


def save_data_to_file(df: pd.DataFrame, file_path: str):
    """
    Сохраняет DataFrame в CSV-файл, избегая дубликатов по дате.
    """
    if df.empty:
        return

    new_date = df['Дата'].iloc[0]

    if os.path.exists(file_path):
        df_history = load_data_from_file(file_path)

        if isinstance(new_date, pd.Timestamp):
            new_date = new_date.date()

        df_history = df_history[df_history['Дата'] != new_date]
        df_combined = pd.concat([df_history, df], ignore_index=True)
    else:
        df_combined = df

    df_combined.to_csv(file_path, index=False)


def parse_and_process_report(report_text: str) -> tuple[pd.DataFrame, pd.DataFrame, date]:
    """
    Парсит текст отчета и возвращает обработанные DataFrame для сохранения.
    """

    # 1. Извлечение даты отчета
    date_match = re.search(r"Отчет ОКК (\d{2}\.\d{2}\.\d{4})", report_text)
    if not date_match:
        raise ValueError("Не удалось найти дату в заголовке отчета.")
    report_date_str = date_match.group(1)
    report_date = datetime.strptime(report_date_str, '%d.%m.%Y').date()

    # --- Парсинг Показателя 1: Задачи по сотрудникам ---
    staff_data = []
    task_regex = r"(\w+) - поставлено (\d+)/выполнено (\d+)"

    for match in re.finditer(task_regex, report_text, re.MULTILINE):
        name, set_tasks, completed_tasks = match.groups()
        staff_data.append({
            'Дата': report_date,
            'Сотрудник': name.strip(),
            'Поставлено': int(set_tasks),
            'Выполнено': int(completed_tasks)
        })

    df_staff = pd.DataFrame(staff_data)

    if not df_staff.empty:
        df_staff = df_staff.groupby(['Дата', 'Сотрудник'], as_index=False).agg({
            'Поставлено': 'sum',
            'Выполнено': 'sum'
        })
        df_staff['% Выполнения'] = (df_staff['Выполнено'] / df_staff['Поставлено'] * 100).round(2).fillna(0)

    # --- Парсинг Показателя 2 и 3: Общие метрики ---
    metrics_data = {'Дата': report_date}

    metrics_patterns = {
        'Пропущенных': r"2\. Пропущенных - (\d+)",
        'Перезвонов > 5 мин': r"Количество перезвонов более 5 минут - (\d+)",
        'Не перезвонили/не написали': r"Не перезвонили/не написали - (\d+)",
        'Просрочено': r"Количество заказов, просроченных обработку - (\d+) / \d+",
        'Всего заказов': r"Количество заказов, просроченных обработку - \d+ / (\d+)",
    }

    for key, pattern in metrics_patterns.items():
        match = re.search(pattern, report_text)
        metrics_data[key] = int(match.group(1)) if match else 0

    df_metrics = pd.DataFrame([metrics_data])

    if df_metrics['Всего заказов'].iloc[0] > 0:
        df_metrics['% Просрочки'] = (df_metrics['Просрочено'] / df_metrics['Всего заказов'] * 100).round(2)
    else:
        df_metrics['% Просрочки'] = 0

    return df_staff, df_metrics, report_date


# --- ФУНКЦИИ ГЕНЕРАЦИИ ГРАФИКОВ И СЛАЙДШОУ ---

def generate_data_dashboard_files(df_metrics_history: pd.DataFrame, df_staff_history: pd.DataFrame,
                                  report_date: date) -> list[str]:
    """
    Генерирует три отдельных HTML-файла для каждого показателя.
    Возвращает список путей к сгенерированным файлам.
    """
    generated_files = []
    report_date_str = report_date.strftime('%d.%m.%Y')

    # 1. Показатель 1: Эффективность выполнения задач по сотрудникам (Текущий день)
    filename_1 = f"{DASHBOARD_PREFIX}_1_staff_{report_date.strftime('%Y-%m-%d')}.html"
    if not df_staff_history.empty:
        df_today = df_staff_history[df_staff_history['Дата'] == report_date].sort_values(by='% Выполнения',
                                                                                         ascending=False)

        if not df_today.empty:
            # Настраиваем цвета для графика задач
            fig_staff = px.bar(df_today, x='Сотрудник', y=['Поставлено', 'Выполнено'],
                               title=f'1. Выполнение задач по сотрудникам ({report_date_str})',
                               barmode='group', text_auto=True,
                               # Назначаем заданные цвета
                               color_discrete_map={'Выполнено': COLOR_COMPLETED,
                                                   'Поставлено': COLOR_MISSED})
            # УСТАНОВКА ОПТИМИЗИРОВАННОЙ ВЫСОТЫ
            fig_staff.update_layout(height=PLOTLY_HEIGHT)

            html_content = f"<h1>1. Эффективность выполнения задач</h1>{fig_staff.to_html(full_html=False, include_plotlyjs='cdn')}"

            with open(filename_1, 'w', encoding='utf-8') as f:
                f.write(generate_plot_html_template(f"ОКК - Задачи {report_date_str}", html_content))
            generated_files.append(filename_1)

    # 2. Показатель 2: Динамика пропущенных звонков
    filename_2 = f"{DASHBOARD_PREFIX}_2_missed_{report_date.strftime('%Y-%m-%d')}.html"
    if not df_metrics_history.empty and len(df_metrics_history) >= 1:
        df_metrics_history_sorted = df_metrics_history.sort_values(by='Дата')
        df_metrics_history_sorted['Дата_Str'] = df_metrics_history_sorted['Дата'].astype(str)

        fig_missed = px.line(df_metrics_history_sorted, x='Дата_Str',
                             y=['Пропущенных', 'Перезвонов > 5 мин', 'Не перезвонили/не написали'],
                             title='2. Динамика пропущенных звонков и задержек',
                             markers=True)
        fig_missed.update_yaxes(title='Количество')
        fig_missed.update_xaxes(title='Дата')
        # УСТАНОВКА ОПТИМИЗИРОВАННОЙ ВЫСОТЫ
        fig_missed.update_layout(height=PLOTLY_HEIGHT)

        html_content = f"<h1>2. Контроль пропущенных звонков</h1>{fig_missed.to_html(full_html=False, include_plotlyjs='cdn')}"

        with open(filename_2, 'w', encoding='utf-8') as f:
            f.write(generate_plot_html_template(f"ОКК - Звонки {report_date_str}", html_content))
        generated_files.append(filename_2)

    # 3. Показатель 3: Динамика просроченных заказов
    filename_3 = f"{DASHBOARD_PREFIX}_3_overdue_{report_date.strftime('%Y-%m-%d')}.html"
    if not df_metrics_history.empty and len(df_metrics_history) >= 1:
        df_metrics_history_sorted = df_metrics_history.sort_values(by='Дата').copy()
        df_metrics_history_sorted['Вовремя'] = (
                df_metrics_history_sorted['Всего заказов'] - df_metrics_history_sorted['Просрочено']
        )
        df_plot = df_metrics_history_sorted[df_metrics_history_sorted['Всего заказов'] > 0].copy()

        if not df_plot.empty:
            df_plot['Дата_Str'] = df_plot['Дата'].astype(str)

            fig_prosr = px.bar(
                df_plot, x='Дата_Str', y=['Вовремя', 'Просрочено'],
                title='3. Контроль просрочки заказов (в штуках)',
                # Используем заданные цвета
                color_discrete_map={'Вовремя': COLOR_COMPLETED, 'Просрочено': COLOR_MISSED}
            )
            fig_prosr.update_layout(yaxis_title="Количество заказов", xaxis_title="Дата", barmode='stack',
                                    legend_title_text='Статус',
                                    height=PLOTLY_HEIGHT)  # УСТАНОВКА ОПТИМИЗИРОВАННОЙ ВЫСОТЫ

            # Добавление аннотаций "Всего"
            for _, row in df_plot.iterrows():
                fig_prosr.add_annotation(
                    x=row['Дата_Str'], y=row['Всего заказов'],
                    text=f"Всего: {row['Всего заказов']}",
                    showarrow=False, yshift=10, font=dict(size=10, color="gray")
                )

            html_content = f"<h1>3. Контроль просрочки заказов</h1>{fig_prosr.to_html(full_html=False, include_plotlyjs='cdn')}"

            with open(filename_3, 'w', encoding='utf-8') as f:
                f.write(generate_plot_html_template(f"ОКК - Просрочка {report_date_str}", html_content))
            generated_files.append(filename_3)

    return generated_files


def generate_plot_html_template(title: str, content: str) -> str:
    """Генерирует общую HTML-обертку для одного графика с учетом фона и размера TV."""
    global BACKGROUND_URL  # Используем глобальную константу
    return f"""
    <html>
    <head>
        <title>{title}</title>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <style>
            /* Стили для Smart TV: растягиваем на весь экран 1366x768 */
            body {{ 
                font-family: 'Inter', sans-serif; 
                margin: 0; 
                padding: 0;
                overflow: hidden; 
                height: 100vh;
                width: 100vw;
                /* УСТАНОВКА ФОНА ПО URL */
                background-image: url('{BACKGROUND_URL}');
                background-size: cover; /* ИЗМЕНЕНИЕ: Фон заполнит весь экран, сохраняя пропорции */
                background-repeat: no-repeat;
                background-attachment: fixed; 
                background-position: center center;
                background-color: #1f2937; /* Запасной темный цвет */
                display: flex;
                flex-direction: column;
                align-items: center;
            }}
            h1 {{ 
                color: white; 
                text-shadow: 2px 2px 4px #000000; 
                text-align: center;
                margin: 10px 0; 
                padding-top: 10px;
                font-size: 2.5em;
            }}
            /* Стиль для контейнера Plotly */
            .plotly-graph-div {{
                width: 1300px !important; /* Ширина под 1366, с учетом небольших полей */
                height: {PLOTLY_HEIGHT}px !important; 
                margin: 0 auto; 
                box-shadow: 0 4px 12px rgba(0, 0, 0, 0.5);
                border-radius: 12px;
                overflow: hidden;
            }}
            /* Стили для самого графика, чтобы он был на белом фоне */
            .js-plotly-plot {{
                background-color: white !important;
                padding: 15px;
                border-radius: 12px;
            }}
        </style>
        <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    </head>
    <body>
        {content}
    </body>
    </html>
    """


def generate_slideshow_host(data_file_paths: list[str], report_date: date) -> str:
    """
    Генерирует HTML-файл (latest_dashboard.html) с логикой циклического слайдшоу.
    """

    # 1. Создаем список URL-адресов графиков (нам нужны только имена файлов)
    iframe_src_list = [os.path.basename(p) for p in data_file_paths]

    global BACKGROUND_URL  # Используем глобальную константу

    # 2. Формируем HTML с JS-логикой
    final_html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>ОКК Дэшборд | Слайдшоу за {report_date.strftime('%d.%m.%Y')}</title>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <script src="https://cdn.tailwindcss.com"></script>
        <style>
            /* Стили для Smart TV: растягиваем на весь экран */
            body, html {{
                margin: 0;
                padding: 0;
                width: 100%;
                height: 100%;
                overflow: hidden;
                font-family: 'Inter', sans-serif;
                /* ИСПОЛЬЗУЕМ ТОТ ЖЕ ФОН ПО URL В ХОСТЕ */
                background-image: url('{BACKGROUND_URL}');
                background-size: cover; /* ИЗМЕНЕНИЕ: Фон заполнит весь экран, сохраняя пропорции */
                background-repeat: no-repeat;
                background-attachment: fixed;
                background-position: center center;
                background-color: #1f2937; 
            }}
            #slideshow-container {{
                width: 100%;
                height: 100%;
                position: relative;
            }}
            .dashboard-iframe {{
                width: 100%;
                height: 100%;
                border: none;
                transition: opacity 0.7s ease-in-out;
                opacity: 0; /* Изначально скрыты */
                position: absolute;
                top: 0;
                left: 0;
                background-color: transparent; 
            }}
            .dashboard-iframe.active {{
                opacity: 1;
            }}
            /* Убедитесь, что все IFRAME готовы принимать полноэкранный контент */
            iframe {{
                pointer-events: none; 
            }}
        </style>
    </head>
    <body>
        <div id="slideshow-container">
            </div>

        <script>
            const files = {iframe_src_list};
            const interval = {SLIDESHOW_INTERVAL_SECONDS} * 1000;
            let currentSlide = 0;
            const container = document.getElementById('slideshow-container');
            let iframes = [];

            // 1. Создание всех iframe
            files.forEach((src, index) => {{
                const iframe = document.createElement('iframe');
                iframe.className = 'dashboard-iframe';
                iframe.src = src;
                iframe.id = 'slide-' + index;
                // ВАЖНО: Добавляем параметр allowfullscreen для возможности переключения в полный экран
                iframe.setAttribute('allowfullscreen', 'true');
                container.appendChild(iframe);
                iframes.push(iframe);
            }});

            // 2. Функция переключения слайдов
            function showSlide(index) {{
                // Скрываем все
                iframes.forEach(iframe => iframe.classList.remove('active'));

                // Показываем текущий
                if (iframes[index]) {{
                    iframes[index].classList.add('active');
                }}
            }}

            // 3. Запуск цикла
            function startSlideshow() {{
                if (iframes.length === 0) return;

                showSlide(currentSlide);

                // Переходим к следующему слайду
                currentSlide = (currentSlide + 1) % iframes.length;

                // Планируем следующий показ
                setTimeout(startSlideshow, interval);
            }}

            // Запускаем через короткую задержку после загрузки
            window.onload = function() {{
                if (iframes.length > 0) {{
                    // Небольшая задержка для загрузки всех iframe
                    setTimeout(startSlideshow, 1000); 
                }} else {{
                    container.innerHTML = '<div style="color: white; padding: 20px; text-align: center;">Нет файлов для слайдшоу. Проверьте генерацию данных.</div>';
                }}
            }};
        </script>
    </body>
    </html>
    """

    # Сохраняем главный файл слайдшоу (который Nginx ждет по умолчанию)
    with open(LATEST_DASHBOARD_FILE, 'w', encoding='utf-8') as f:
        f.write(final_html)

    return LATEST_DASHBOARD_FILE


# --- ОСНОВНАЯ ФУНКЦИЯ ГЕНЕРАЦИИ ---

def generate_dashboard_from_text(report_text_input: str) -> str | None:
    """
    Основная функция для генерации дашборда, вызываемая из бота.
    """
    try:
        # 1. Парсинг отчета
        df_staff_new, df_metrics_new, current_date = parse_and_process_report(report_text_input)
        print(f"✅ Отчет за {current_date.strftime('%d.%m.%Y')} успешно проанализирован.")

        # 2. Сохранение данных в историю
        df_staff_new['Дата'] = df_staff_new['Дата'].apply(lambda x: x.date() if isinstance(x, datetime) else x)
        df_metrics_new['Дата'] = df_metrics_new['Дата'].apply(lambda x: x.date() if isinstance(x, datetime) else x)

        save_data_to_file(df_staff_new, STAFF_HISTORY_FILE)
        save_data_to_file(df_metrics_new, METRICS_HISTORY_FILE)

        # 3. Загрузка всей истории
        df_staff_history = load_data_from_file(STAFF_HISTORY_FILE)
        df_metrics_history = load_data_from_file(METRICS_HISTORY_FILE)

        if df_metrics_history.empty and df_staff_history.empty:
            print("⚠️ Недостаточно данных для построения графиков.")
            return None

        # 4. Генерация 3 отдельных файлов с графиками
        data_dashboard_files = generate_data_dashboard_files(df_metrics_history, df_staff_history, current_date)

        # 5. Генерация файла-хоста слайдшоу (latest_dashboard.html)
        slideshow_host_file = generate_slideshow_host(data_dashboard_files, current_date)

        # 6. --- ВЫЗОВ ЗАГРУЗКИ НА ХОСТИНГ ---
        load_dotenv()
        remote_path = os.getenv('SFTP_PATH', '/')

        # Фон загружается по URL, поэтому в список файлов для SFTP он не включается
        all_files_to_upload = data_dashboard_files + [slideshow_host_file]
        upload_files_to_sftp(all_files_to_upload, remote_path)

        return slideshow_host_file

    except ValueError as e:
        raise ValueError(f"Ошибка парсинга отчета. Проверьте формат. Детали: {e}")
    except Exception as e:
        raise Exception(f"Неизвестная ошибка генерации дашборда: {e}")


# --- КОНСОЛЬНЫЙ ЗАПУСК ---

if __name__ == "__main__":
    print("--- Запуск генератора дэшборда (консольный режим) ---")
    if not os.path.exists(INPUT_REPORT_FILE):
        print(f"❌ Ошибка: Файл отчета '{INPUT_REPORT_FILE}' не найден.")
        exit()

    try:
        with open(INPUT_REPORT_FILE, 'r', encoding='utf-8') as f:
            report_text_input = f.read()
    except Exception as e:
        print(f"❌ Ошибка при чтении файла '{INPUT_REPORT_FILE}': {e}")
        exit()

    try:
        html_file = generate_dashboard_from_text(report_text_input)
        if html_file:
            print(f"✨ Слайдшоу дэшбордов успешно создано: {html_file}")
    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")

    print("--- Генерация завершена ---")
