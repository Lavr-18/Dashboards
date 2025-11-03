import pandas as pd
import re
import json
from datetime import datetime, date, timedelta
import os
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import paramiko
from dotenv import load_dotenv
import io
import requests
from dateutil.relativedelta import relativedelta
from collections import defaultdict
import time

# --- КОНСТАНТЫ И КОНФИГУРАЦИЯ ---
INPUT_REPORT_FILE = 'latest_report.txt'
STAFF_HISTORY_FILE = 'staff_report_history.csv'
METRICS_HISTORY_FILE = 'metrics_report_history.csv'
OVERDUE_TASKS_MONTHLY_FILE = 'overdue_tasks_monthly.json'
LATEST_DASHBOARD_FILE = 'latest_dashboard.html'
SLIDESHOW_INTERVAL_SECONDS = 15
DASHBOARD_PREFIX = 'dashboard_data'
DASHBOARD_PREFIX_GS = 'dashboard_gs_data'
NEW_FILES_LIST = []
REFRESH_INTERVAL_SECONDS = 3600  # Обновление каждый час (для внешних графиков)

# --- НОВЫЕ КОНСТАНТЫ ДЛЯ RETAILCRM ---
TASK_FILTER_TEXT_LOWER = "связаться с клиентом"
TASK_FILTER_TEXT_UPPER = "Связаться с клиентом"
MANAGER_CACHE = {}  # Кэш для имен менеджеров RetailCRM

# --- КОНСТАНТЫ СТИЛИЗАЦИИ ---
COLOR_COMPLETED = 'rgb(136, 190, 67)'  # Выполнено/Вовремя (зеленый)
COLOR_MISSED = 'rgb(240, 102, 0)'  # Не выполнено / Просрочено (оранжевый)
PLOTLY_HEIGHT = 550
PLOTLY_WIDTH = 950  # 950px - максимальная ширина для графика
CUSTOM_COLORS = ['#F06600', '#88BE43', '#813591']

# --- КОНСТАНТА ДЛЯ URL ФОНА ---
BACKGROUND_URL = 'https://disk.yandex.ru/i/wAjsKqMrRGPpkQ'

# --- НОВЫЕ КОНСТАНТЫ ДЛЯ GOOGLE SHEETS ---
GOOGLE_SHEET_EXPORT_URL = "https://docs.google.com/spreadsheets/d/1gRE19ub6gQz6o9yKEGgaESvN3oN52BRad-X2dYgrUEw/export?format=xlsx"


# --- УТИЛИТЫ ДЛЯ JSON И ДАТЫ ---

def get_current_month_key():
    """Возвращает ключ текущего месяца в формате 'YYYY-MM'."""
    return datetime.now().strftime('%Y-%m')


def load_monthly_overdue_data():
    """Загружает данные о месячной просрочке из JSON-файла."""
    if os.path.exists(OVERDUE_TASKS_MONTHLY_FILE):
        try:
            with open(OVERDUE_TASKS_MONTHLY_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if not isinstance(data, dict):
                    print("Предупреждение: JSON файл просрочки поврежден, создаем новый.")
                    return {}
                return data
        except json.JSONDecodeError:
            print("Предупреждение: Ошибка декодирования JSON, создаем новый файл.")
            return {}
    return {}


def calculate_and_update_monthly_overdue(daily_overdue_data):
    """
    Обновляет накопительные данные о просроченных задачах за текущий месяц
    (на основе данных, полученных из отчета).
    """
    monthly_data = load_monthly_overdue_data()
    current_month_key = get_current_month_key()

    # Инициализация для текущего месяца
    if current_month_key not in monthly_data:
        monthly_data[current_month_key] = defaultdict(int)

    if not isinstance(monthly_data[current_month_key], defaultdict):
        monthly_data[current_month_key] = defaultdict(int, monthly_data[current_month_key])

    # Обновление данных за сегодня
    for manager, overdue_count in daily_overdue_data.items():
        # Если менеджер уже был в списке сегодня, мы не хотим его дублировать,
        # но поскольку это данные из отчета, мы предполагаем, что этот запуск
        # переписывает данные за текущий день.
        monthly_data[current_month_key][manager] += overdue_count

    # Преобразование defaultdict обратно в dict для сериализации
    data_to_save = {
        month: dict(tasks) if isinstance(tasks, defaultdict) else tasks
        for month, tasks in monthly_data.items()
    }

    try:
        with open(OVERDUE_TASKS_MONTHLY_FILE, 'w', encoding='utf-8') as f:
            json.dump(data_to_save, f, ensure_ascii=False, indent=4)
    except Exception as e:
        print(f"Ошибка записи в JSON файл накопительной просрочки: {e}")

    return monthly_data


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
            sftp.put(local_path, remote_path)
            print(f"⬆️ Успешно загружен {os.path.basename(local_path)} на {SFTP_HOST}")

        sftp.close()
        transport.close()
        return True

    except Exception as e:
        print(f"❌ Критическая ошибка SFTP-загрузки: {e}")
        return False


# --- УТИЛИТЫ ДЛЯ API RETAILCRM (НОВЫЙ БЛОК) ---

def api_call_with_backoff(url, params=None, max_retries=5):
    """Выполняет GET-запрос с экспоненциальным отходом."""
    for attempt in range(max_retries):
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"⚠️ Ошибка API при запросе {url} (Попытка {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                sleep_time = 2 ** attempt
                print(f"   Ожидание {sleep_time} секунд перед повтором...")
                time.sleep(sleep_time)
            else:
                raise Exception(f"Критическая ошибка API после {max_retries} попыток: {e}")


def get_retailcrm_manager_name(user_id: int, base_url: str, api_key: str) -> str:
    """
    Получает имя менеджера по ID и кэширует результат.
    Формат: FirstName + первая буква LastName.
    """
    if user_id in MANAGER_CACHE:
        return MANAGER_CACHE[user_id]

    url = f"{base_url}/api/v5/users/{user_id}"
    params = {'apiKey': api_key}

    try:
        data = api_call_with_backoff(url, params=params)

        if data.get('success') and 'user' in data:
            user = data['user']
            first_name = user.get('firstName', '')
            last_name = user.get('lastName', '')

            if first_name and last_name:
                formatted_name = f"{first_name} {last_name[0].upper()}."
                MANAGER_CACHE[user_id] = formatted_name
                return formatted_name
            elif first_name:
                MANAGER_CACHE[user_id] = first_name
                return first_name
    except Exception as e:
        # Для случаев, когда пользователь может быть удален или не найден
        print(f"❌ Ошибка при получении имени менеджера (ID: {user_id}): {e}")

        # Если не удалось получить имя, возвращаем ID
    return f"Менеджер #{user_id}"


def get_month_range(target_date: date) -> tuple[str, str]:
    """Возвращает начальную и конечную дату текущего месяца в формате 'YYYY-MM-DD'."""
    start_of_month = target_date.replace(day=1)
    # Конец месяца, включительно
    end_of_month = start_of_month + relativedelta(months=1) - timedelta(seconds=1)

    return start_of_month.strftime('%Y-%m-%d'), end_of_month.strftime('%Y-%m-%d')


def fetch_retailcrm_tasks(base_url: str, api_key: str, start_date: str, end_date: str) -> list:
    """
    Извлекает все задачи RetailCRM с учетом пагинации и фильтрации.
    """
    print(f"🔄 Запрос задач RetailCRM за период: {start_date} - {end_date}...")

    url = f"{base_url}/api/v5/tasks"
    all_tasks = []
    current_page = 1
    total_page_count = 1

    params = {
        'apiKey': api_key,
        # Используем createdAtFrom/To для фильтрации по месяцу,
        # т.к. RetailCRM не имеет фильтра по due_datetime.
        # Это даст нам все задачи, созданные в этом месяце.
        'filter[createdAtFrom]': start_date,
        'filter[createdAtTo]': end_date,
        'filter[text]': TASK_FILTER_TEXT_LOWER,  # Используем нижний регистр для широкой фильтрации на стороне API
        'limit': 100,
        'page': current_page
    }

    while current_page <= total_page_count:
        params['page'] = current_page

        try:
            data = api_call_with_backoff(url, params=params)
        except Exception as e:
            print(f"❌ Не удалось получить данные о задачах со страницы {current_page}. Остановка.")
            break

        if not data.get('success'):
            print("❌ Ошибка RetailCRM API: success=false.")
            return []

        tasks = data.get('tasks', [])
        all_tasks.extend(tasks)

        pagination = data.get('pagination', {})
        total_page_count = pagination.get('totalPageCount', 1)

        if current_page >= total_page_count:
            break

        current_page += 1

    print(f"✅ Получено {len(all_tasks)} задач со всех страниц.")
    return all_tasks


def process_tasks_for_chart_6(tasks: list, base_url: str, api_key: str) -> dict:
    """
    Обрабатывает список задач, фильтрует просроченные задачи по менеджеру.
    """

    overdue_tasks_by_manager = defaultdict(int)
    now = datetime.now()

    for task in tasks:
        # 1. Строгая фильтрация по тексту (с учетом регистра)
        task_text = task.get('text', '')
        if task_text not in [TASK_FILTER_TEXT_LOWER, TASK_FILTER_TEXT_UPPER]:
            continue

        # 2. Фильтрация по типу исполнителя: только 'user'
        if task.get('performerType') != 'user':
            continue

        performer_id = task.get('performer')
        if not performer_id:
            continue

        # 3. Определение просрочки (логика по ТЗ)
        is_overdue = False
        due_datetime_str = task.get('datetime')
        complete = task.get('complete', False)
        completed_at_str = task.get('completedAt')

        if not due_datetime_str:
            continue

        try:
            # Срок, когда задача должна быть выполнена
            due_datetime = datetime.strptime(due_datetime_str, '%Y-%m-%d %H:%M')
        except ValueError:
            continue

        if not complete:
            # Задача не выполнена и срок прошел
            if due_datetime < now:
                is_overdue = True
        else:
            # Задача выполнена, но просрочена
            if completed_at_str:
                try:
                    # Фактическая дата выполнения
                    completed_at = datetime.strptime(completed_at_str, '%Y-%m-%d %H:%M:%S')

                    # Просрочена, если выполнена позже срока
                    if completed_at > due_datetime:
                        is_overdue = True
                except ValueError:
                    pass

        if is_overdue:
            manager_name = get_retailcrm_manager_name(performer_id, base_url, api_key)
            overdue_tasks_by_manager[manager_name] += 1

    return dict(overdue_tasks_by_manager)


def generate_chart_6(overdue_data: dict, report_date: date) -> str:
    """
    Генерирует Plotly график 6: Просроченные задачи по менеджерам за месяц.
    """

    current_date_str = report_date.strftime('%d.%m.%Y')

    # Сортируем данные по убыванию просрочки
    sorted_data = sorted(overdue_data.items(), key=lambda item: item[1], reverse=True)
    df = pd.DataFrame(sorted_data, columns=['Менеджер', 'Просрочено'])

    filename = f"{DASHBOARD_PREFIX}_6_monthly_crm_tasks_{report_date.strftime('%Y-%m-%d_%H%M%S')}.html"

    if df.empty or df['Просрочено'].sum() == 0:
        print("⚠️ График 6: Нет просроченных задач для отображения.")
        # Создаем пустой заглушечный график
        fig = go.Figure()
        fig.add_annotation(
            x=0.5, y=0.5,
            text="6. Нет просроченных задач по фильтру 'Связаться с клиентом' за месяц.",
            showarrow=False,
            font=dict(size=20, color="gray")
        )
        fig.update_layout(height=PLOTLY_HEIGHT, width=PLOTLY_WIDTH, template="plotly_white")
    else:
        fig = px.bar(
            df, x='Менеджер', y='Просрочено', text='Просрочено',
            title='6. Просроченные задачи "Связаться с клиентом" по менеджерам (за месяц)',
            color_discrete_sequence=[COLOR_MISSED]
        )

        fig.update_traces(textposition='outside')
        fig.update_layout(
            yaxis_title="Количество просроченных задач",
            xaxis_title="Менеджер",
            height=PLOTLY_HEIGHT,
            width=PLOTLY_WIDTH,
            template="plotly_white",
            uniformtext_minsize=10,
            uniformtext_mode='hide',
        )
        fig.update_xaxes(tickangle=45)
        fig.update_yaxes(rangemode='tozero')

    html_content = f"{fig.to_html(full_html=False, include_plotlyjs='cdn')}"

    with open(filename, 'w', encoding='utf-8') as f:
        f.write(generate_plot_html_template(f"ОКК - Просрочка CRM {current_date_str}", html_content))

    return filename


# --- ФУНКЦИИ РАБОТЫ С ДАННЫМИ (частично изменены) ---

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


def parse_uncompleted_tasks_for_chart(report_text):
    """
    Парсит секцию 1 отчета для извлечения 'поставлено' и 'выполнено'
    для расчета просроченных задач.
    """

    # Ищем начало секции 1, чтобы обрезать остальной текст
    start_match = re.search(r"1\.\s+Проверка невыполненных задач:\s*\d+", report_text)
    if not start_match:
        # Возвращаем пустой DataFrame и пустой defaultdict, если секция не найдена
        return pd.DataFrame(columns=['Manager', 'Posted', 'Completed', 'Overdue']), defaultdict(int)

    # Обрезаем текст, оставляя только нужную часть
    tasks_section = report_text[start_match.end():]

    # Паттерн для поиска строки: Имя - поставлено N/выполнено M (...)
    pattern = re.compile(
        r"\s*([А-ЯЁа-яё\s-]+)\s*-\s*поставлено\s*(\d+)/выполнено\s*(\d+)\s*.*",
        re.MULTILINE
    )

    data = []

    for match in pattern.finditer(tasks_section):
        manager = match.group(1).strip()
        posted = int(match.group(2))
        completed = int(match.group(3))

        overdue = posted - completed

        data.append({
            'Manager': manager,
            'Posted': posted,
            'Completed': completed,
            'Overdue': overdue if overdue > 0 else 0
        })

    if not data:
        return pd.DataFrame(columns=['Manager', 'Posted', 'Completed', 'Overdue']), defaultdict(int)

    df = pd.DataFrame(data)
    # Группируем, чтобы суммировать задачи, если менеджер упоминается несколько раз (как в примере: Екатерина)
    df = df.groupby('Manager').sum(numeric_only=True).reset_index()
    # Пересчитываем просрочку после группировки
    df['Overdue'] = df['Posted'] - df['Completed']
    df.loc[df['Overdue'] < 0, 'Overdue'] = 0

    # Создаем словарь только для просроченных задач (> 0)
    daily_overdue_data = df[df['Overdue'] > 0].set_index('Manager')['Overdue'].to_dict()
    daily_overdue_data = defaultdict(int, daily_overdue_data)

    return df, daily_overdue_data


def parse_and_process_report(report_text: str) -> tuple[pd.DataFrame, pd.DataFrame, date]:
    """
    Парсит текст отчета и возвращает обработанные DataFrame для сохранения.
    """

    # 1. Извлечение даты отчета
    date_match = re.search(r"Отчет ОКК (\d{2}\.\d{2}\.\d{4})", report_text)
    if not date_match:
        report_date = date.today()
    else:
        report_date_str = date_match.group(1)
        report_date = datetime.strptime(report_date_str, '%d.%m.%Y').date()

    # --- Парсинг Показателя 1: Задачи по сотрудникам (Без изменений) ---
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
        'Просрочено': r"Количество заказов, просроченных обработку - (\d+)",
    }

    # Сначала парсим основные метрики
    for key, pattern in metrics_patterns.items():
        match = re.search(pattern, report_text)
        metrics_data[key] = int(match.group(1)) if match else 0

    # Отдельно парсим "Всего заказов". Он может отсутствовать.
    total_orders_match = re.search(r"Количество заказов, просроченных обработку - \d+\s*(?:/\s*(\d+))?", report_text)

    # Если найден общий паттерн и в нем есть второе число (Всего заказов)
    if total_orders_match and total_orders_match.group(1):
        metrics_data['Всего заказов'] = int(total_orders_match.group(1))
    else:
        # Если "Всего заказов" не найдено в паттерне "X / Y", ставим 0.
        metrics_data['Всего заказов'] = 0

    df_metrics = pd.DataFrame([metrics_data])

    if df_metrics['Всего заказов'].iloc[0] > 0:
        df_metrics['% Просрочки'] = (df_metrics['Просрочено'] / df_metrics['Всего заказов'] * 100).round(2)
    else:
        df_metrics['% Просрочки'] = 0

    return df_staff, df_metrics, report_date


def generate_overdue_tasks_dual_chart(df_daily, monthly_overdue_data, report_date: date):
    """
    Генерирует Plotly график с двумя столбчатыми диаграммами в ряд:
    1. Ежедневная просрочка (Просроченные > 0).
    2. Месячная накопительная просрочка (Накопительные > 0).
    """

    current_date_str = report_date.strftime('%d.%m.%Y')
    current_month_key = get_current_month_key()

    # 1. Данные для Дневной просрочки (только те, где просрочено > 0)
    df_daily_overdue = df_daily[df_daily['Overdue'] > 0].sort_values(by='Overdue', ascending=False)

    # 2. Данные для Месячной накопительной просрочки
    monthly_tasks = monthly_overdue_data.get(current_month_key, {})
    df_monthly_overdue = pd.DataFrame(list(monthly_tasks.items()),
                                      columns=['Manager', 'Monthly_Overdue'])
    df_monthly_overdue = df_monthly_overdue[df_monthly_overdue['Monthly_Overdue'] > 0].sort_values(
        by='Monthly_Overdue', ascending=False
    )

    # --- Настройка размеров ---
    total_width = PLOTLY_WIDTH
    total_height = PLOTLY_HEIGHT

    fig = make_subplots(rows=1, cols=2,
                        subplot_titles=(f"Просроченные задачи сегодня ({current_date_str})",
                                        "Накопительная просрочка за месяц (из отчетов)"),
                        horizontal_spacing=0.08)

    # --- ГРАФИК 1: Ежедневная просрочка ---
    fig1 = px.bar(df_daily_overdue,
                  x='Manager',
                  y='Overdue',
                  text='Overdue')

    for trace in fig1.data:
        trace.marker.color = COLOR_MISSED
        fig.add_trace(trace, row=1, col=1)
        fig.update_xaxes(title_text="Менеджер", row=1, col=1)
        fig.update_yaxes(title_text="Кол-во просроченных задач", row=1, col=1)

    # --- ГРАФИК 2: Месячная накопительная просрочка ---
    fig2 = px.bar(df_monthly_overdue,
                  x='Manager',
                  y='Monthly_Overdue',
                  text='Monthly_Overdue')

    for trace in fig2.data:
        trace.marker.color = COLOR_MISSED
        fig.add_trace(trace, row=1, col=2)
        fig.update_xaxes(title_text="Менеджер", row=1, col=2)
        fig.update_yaxes(title_text="Накопительная просрочка (Месяц)", row=1, col=2)

    # Общие настройки макета
    fig.update_layout(
        title_text=f"1. Анализ просроченных задач (Дневная и Месячная динамика)",
        height=total_height,
        width=total_width,
        template="plotly_white",
        uniformtext_minsize=10,
        uniformtext_mode='hide',
    )

    fig.update_traces(textposition='outside')
    fig.update_xaxes(tickangle=45)  # Небольшой наклон для имен
    fig.update_yaxes(rangemode='tozero')

    # Сохранение графика
    chart_filename = f'{DASHBOARD_PREFIX}_1_tasks_dual_{report_date.strftime("%Y-%m-%d")}.html'

    html_content = f"{fig.to_html(full_html=False, include_plotlyjs='cdn')}"
    with open(chart_filename, 'w', encoding='utf-8') as f:
        # Используем width_override, чтобы HTML-шаблон знал, что это 950px, а не 950px*2
        f.write(generate_plot_html_template(f"ОКК - Задачи (Dual) {current_date_str}", html_content,
                                            width_override=total_width))

    return chart_filename


def generate_data_dashboard_files(df_metrics_history: pd.DataFrame, report_date: date) -> list[str]:
    """
    Генерирует два отдельных HTML-файла для показателей 2 и 3.
    """
    generated_files = []
    report_date_str = report_date.strftime('%d.%m.%Y')

    # 2. Показатель 2: Динамика пропущенных звонков (без изменений)
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
        fig_missed.update_layout(height=PLOTLY_HEIGHT, width=PLOTLY_WIDTH,
                                 legend_title_text='Метрики')

        html_content = f"{fig_missed.to_html(full_html=False, include_plotlyjs='cdn')}"

        with open(filename_2, 'w', encoding='utf-8') as f:
            f.write(generate_plot_html_template(f"ОКК - Звонки {report_date_str}", html_content))
        generated_files.append(filename_2)

    # 3. Показатель 3: Динамика просроченных заказов
    filename_3 = f"{DASHBOARD_PREFIX}_3_overdue_{report_date.strftime('%Y-%m-%d')}.html"

    if not df_metrics_history.empty and len(df_metrics_history) >= 1:

        # --- ФИЛЬТРАЦИЯ ДАННЫХ ПО ПОСЛЕДНИМ 3 ДНЯМ ---
        three_days_ago = date.today() - relativedelta(days=2)
        df_plot_history = df_metrics_history[df_metrics_history['Дата'] >= three_days_ago].sort_values(by='Дата').copy()

        # Строим график ТОЛЬКО по 'Просрочено'
        df_plot = df_plot_history.copy()

        if not df_plot.empty:
            df_plot['Дата_Str'] = df_plot['Дата'].astype(str)

            fig_prosr = px.bar(
                df_plot, x='Дата_Str', y='Просрочено',  # Используем только 'Просрочено'
                title='3. Динамика просроченных заказов (последние 3 дня)',
                color_discrete_sequence=[COLOR_MISSED]  # Оставляем только оранжевый цвет
            )
            fig_prosr.update_layout(yaxis_title="Количество просроченных заказов", xaxis_title="Дата",
                                    legend_title_text='Статус',
                                    height=PLOTLY_HEIGHT,
                                    width=PLOTLY_WIDTH)

            # Добавление подписей значений
            fig_prosr.update_traces(text=df_plot['Просрочено'], textposition='outside')

            html_content = f"{fig_prosr.to_html(full_html=False, include_plotlyjs='cdn')}"

            with open(filename_3, 'w', encoding='utf-8') as f:
                f.write(generate_plot_html_template(f"ОКК - Просрочка {report_date_str}", html_content))
            generated_files.append(filename_3)

    return generated_files


# --- ФУНКЦИИ GOOGLE SHEETS (без изменений) ---
def download_and_process_google_sheet() -> list[str]:
    """
    Скачивает Google Sheet в формате XLSX, обрабатывает данные и генерирует два новых HTML-файла.
    """

    def format_manager_name(full_name):
        if not isinstance(full_name, str):
            return full_name

        parts = full_name.strip().split()

        if len(parts) >= 2:
            name_part = parts[1].strip()
            surname_initial = parts[0].strip()[0].upper() + '.'

            return f"{name_part} {surname_initial}"

        return full_name

    current_date = date.today()
    generated_files = []

    print("🔄 Начинается загрузка и обработка Google Таблицы...")
    try:
        response = requests.get(GOOGLE_SHEET_EXPORT_URL)
        response.raise_for_status()

        xlsx_data = io.BytesIO(response.content)

        df_daily = pd.read_excel(xlsx_data, sheet_name='Ежедневный_Ввод', engine='openpyxl')
        df_daily.columns = df_daily.columns.str.strip()
        df_daily = df_daily.rename(columns={'Оплачено всего (Р)': 'Оплачено Всего (Р)'})
        df_daily['Дата'] = pd.to_datetime(df_daily['Дата'], errors='coerce')
        df_daily = df_daily.dropna(subset=['Дата', 'Менеджер'])

        df_daily['Менеджер'] = df_daily['Менеджер'].apply(format_manager_name)

        df_manual = pd.read_excel(xlsx_data, sheet_name='Сводка_Текущая', engine='openpyxl',
                                  header=None, skiprows=1, usecols=[0, 1])
        df_manual.columns = ['Менеджер', 'На согласовании (Р)']
        df_manual = df_manual.dropna(subset=['Менеджер'])

        df_manual['Менеджер'] = df_manual['Менеджер'].apply(format_manager_name)

    except Exception as e:
        print(f"❌ Ошибка при загрузке или чтении Google Sheet: {e}")
        return []

    # --- ГРАФИК 4 (Новый 1): Итоги за месяц ---
    filename_gs_1 = f"{DASHBOARD_PREFIX_GS}_1_monthly_{current_date.strftime('%Y-%m-%d_%H%M%S')}.html"

    start_of_month = pd.Timestamp(current_date).to_period('M').start_time
    df_daily_filtered = df_daily[df_daily['Дата'] >= start_of_month]

    df_agg_month = df_daily_filtered.groupby('Менеджер').agg({
        'Оплачено Всего (Р)': 'sum',
        'Отгружено (Факт, Р)': 'sum'
    }).reset_index()

    df_result = pd.merge(df_agg_month, df_manual, on='Менеджер', how='left').fillna(0)

    if not df_result.empty:
        df_plot = df_result.set_index('Менеджер').stack().reset_index()
        df_plot.columns = ['Менеджер', 'Метрика', 'Сумма (Р)']

        fig_month = px.bar(df_plot, x='Менеджер', y='Сумма (Р)', color='Метрика',
                           barmode='group',
                           title=f'4. Итоги за месяц (С {start_of_month.strftime("%d.%m")})',
                           height=PLOTLY_HEIGHT, width=PLOTLY_WIDTH,
                           color_discrete_sequence=CUSTOM_COLORS)

        fig_month.update_layout(yaxis_tickformat=", .0f",
                                hoverlabel_namelength=-1,
                                legend_title_text='Метрика')

        fig_month.update_yaxes(title_text="Сумма (Руб.)", ticksuffix=" ₽")
        fig_month.update_xaxes(tickfont=dict(size=10, weight='bold'))

        html_content = f"{fig_month.to_html(full_html=False, include_plotlyjs='cdn')}"

        with open(filename_gs_1, 'w', encoding='utf-8') as f:
            f.write(generate_plot_html_template(f"ОКК - Месяц {current_date.strftime('%d.%m')}", html_content))
        generated_files.append(filename_gs_1)

    # --- ГРАФИК 5 (Новый 2): День в день ---
    filename_gs_2 = f"{DASHBOARD_PREFIX_GS}_2_daily_{current_date.strftime('%Y-%m-%d_%H%M%S')}.html"

    # --- ФИЛЬТРАЦИЯ ДАННЫХ: только за текущий день ---
    today_date_only = pd.to_datetime(current_date).date()
    df_daily_today = df_daily[df_daily['Дата'].dt.date == today_date_only].copy()

    df_daily_grouped = df_daily_today.groupby([df_daily_today['Дата'].dt.date, 'Менеджер']).agg({
        'Поступило (Лиды, Р)': 'sum',
        'Оплачено Новые (Р)': 'sum',
        'Оплачено Всего (Р)': 'sum'
    }).reset_index()
    df_daily_grouped.rename(columns={'Дата': 'Дата_Факт'}, inplace=True)

    df_long = df_daily_grouped.melt(
        id_vars=['Дата_Факт', 'Менеджер'],
        value_vars=['Поступило (Лиды, Р)', 'Оплачено Новые (Р)', 'Оплачено Всего (Р)'],
        var_name='Метрика',
        value_name='Сумма (Р)'
    )

    if not df_long.empty:
        new_width = PLOTLY_WIDTH
        wrap_columns = 7

        # --- ВОЗВРАТ К ИСХОДНОЙ ВИЗУАЛИЗАЦИИ (Дата на оси X) ---
        fig_daily = px.bar(df_long, x='Дата_Факт', y='Сумма (Р)', color='Метрика', # ВОЗВРАЩАЕМ x='Дата_Факт'
                           facet_col='Менеджер',
                           facet_col_wrap=wrap_columns,
                           barmode='group',
                           title=f'5. Ежедневная динамика (Данные за {current_date.strftime("%d.%m.%Y")})',
                           height=PLOTLY_HEIGHT,
                           width=new_width,
                           color_discrete_sequence=CUSTOM_COLORS)

        fig_daily.update_layout(yaxis_tickformat=", .0f",
                                hoverlabel_namelength=-1,
                                legend_title_text='Метрика',
                                xaxis_title="") # Убираем showlegend=False

        # ВОССТАНАВЛИВАЕМ ИСХОДНЫЕ НАСТРОЙКИ ОСЕЙ X и Y
        fig_daily.update_xaxes(
            matches=None,
            showticklabels=False, # Скрываем подписи даты (они все равно одинаковые)
            title_text="",
            showgrid=False
        )

        fig_daily.update_yaxes(
            title_text="",
            ticksuffix="",
            showticklabels=False
        )

        fig_daily.update_yaxes(
            ticksuffix=" ₽",
            showticklabels=True,
            col=1
        )

        fig_daily.for_each_annotation(lambda a: a.update(
            text=a.text.split("=")[-1].strip(),
            font=dict(size=16, weight='bold')
        ))

        html_content = f"{fig_daily.to_html(full_html=False, include_plotlyjs='cdn')}"

        with open(filename_gs_2, 'w', encoding='utf-8') as f:
            f.write(generate_plot_html_template(f"ОКК - Ежедневная {current_date.strftime('%d.%m')}", html_content))
        generated_files.append(filename_gs_2)
    else:
        # --- ДОБАВЛЕННЫЙ ЛОГ: если нет данных за сегодня ---
        print(f"⚠️ График 5: Пропуск генерации, так как нет данных за {current_date.strftime('%d.%m.%Y')} в Google Sheet.")


    global NEW_FILES_LIST
    NEW_FILES_LIST = generated_files
    print(f"✅ Успешно сгенерировано {len(generated_files)} новых графиков Google Sheets.")
    return generated_files


def generate_plot_html_template(title: str, content: str, width_override: int = None) -> str:
    """
    Генерирует общую HTML-обертку для одного графика.
    Использует width_override для двойного графика (теперь 950px).
    """
    global BACKGROUND_URL
    global PLOTLY_HEIGHT
    default_width = PLOTLY_WIDTH
    final_width = width_override if width_override else default_width

    return f"""
    <html>
    <head>
        <title>{title}</title>
        <meta charset="utf-8">
        <meta name="viewport" content="width=1000, initial-scale=1.0"> 
        <style>
            body {{ 
                font-family: 'Inter', sans-serif; 
                margin: 0; 
                padding: 0;
                overflow: hidden; 
                height: 700px;
                width: 1000px; 
                background-image: url('{BACKGROUND_URL}');
                background-size: cover;
                background-repeat: no-repeat;
                background-attachment: fixed; 
                background-position: center center;
                background-color: #1f2937;
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
            .plotly-graph-div {{
                width: {final_width}px !important; 
                height: {PLOTLY_HEIGHT}px !important; 
                margin: 0 auto; 
                box-shadow: 0 4px 12px rgba(0, 0, 0, 0.5);
                border-radius: 12px;
                overflow: hidden;
            }}
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
    Генерирует HTML-файл (latest_dashboard.html) с логикой циклического слайдшоу
    и мета-тегом для автоматического обновления.
    """

    global NEW_FILES_LIST
    global REFRESH_INTERVAL_SECONDS
    all_files_to_display = data_file_paths + NEW_FILES_LIST

    iframe_src_list = [os.path.basename(p) for p in all_files_to_display]

    global BACKGROUND_URL

    final_html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>ОКК Дэшборд | Слайдшоу за {report_date.strftime('%d.%m.%Y')}</title>
        <meta charset="utf-8">
        <meta name="viewport" content="width=1000, initial-scale=1.0"> 
        <meta http-equiv="refresh" content="{REFRESH_INTERVAL_SECONDS}"> <script src="https://cdn.tailwindcss.com"></script>
        <style>
            body, html {{
                margin: 0;
                padding: 0;
                width: 1000px;
                height: 700px;
                overflow: hidden;
                font-family: 'Inter', sans-serif;
                background-image: url('{BACKGROUND_URL}');
                background-size: cover;
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
                opacity: 0; 
                position: absolute;
                top: 0;
                left: 0;
                background-color: transparent; 
            }}
            .dashboard-iframe.active {{
                opacity: 1;
            }}
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

            files.forEach((src, index) => {{
                const iframe = document.createElement('iframe');
                iframe.className = 'dashboard-iframe';
                iframe.src = src;
                iframe.id = 'slide-' + index;
                iframe.setAttribute('allowfullscreen', 'true');
                container.appendChild(iframe);
                iframes.push(iframe);
            }});

            function showSlide(index) {{
                iframes.forEach(iframe => iframe.classList.remove('active'));

                if (iframes[index]) {{
                    iframes[index].classList.add('active');
                }}
            }}

            function startSlideshow() {{
                if (iframes.length === 0) return;

                showSlide(currentSlide);

                currentSlide = (currentSlide + 1) % iframes.length;

                setTimeout(startSlideshow, interval);
            }}

            window.onload = function() {{
                if (iframes.length > 0) {{
                    setTimeout(startSlideshow, 1000); 
                }} else {{
                    container.innerHTML = '<div style="color: white; padding: 20px; text-align: center;">Нет файлов для слайдшоу. Проверьте генерацию данных.</div>';
                }}
            }};
        </script>
    </body>
    </html>
    """

    with open(LATEST_DASHBOARD_FILE, 'w', encoding='utf-8') as f:
        f.write(final_html)

    return LATEST_DASHBOARD_FILE


def generate_dashboard_from_text(report_text_input: str) -> str | None:
    """
    Основная функция для генерации дашборда, вызываемая из бота.
    """
    load_dotenv()
    RETAILCRM_BASE_URL = os.getenv('RETAILCRM_BASE_URL')
    RETAILCRM_API_KEY = os.getenv('RETAILCRM_API_KEY')

    try:
        # 1. Парсинг отчета для исторического сохранения и извлечение даты
        df_staff_new, df_metrics_new, current_date = parse_and_process_report(report_text_input)
        print(f"✅ Отчет за {current_date.strftime('%d.%m.%Y')} успешно проанализирован (история).")

        # 2. Сохранение данных в историю
        df_staff_new['Дата'] = df_staff_new['Дата'].apply(lambda x: x.date() if isinstance(x, datetime) else x)
        df_metrics_new['Дата'] = df_metrics_new['Дата'].apply(lambda x: x.date() if isinstance(x, datetime) else x)

        save_data_to_file(df_staff_new, STAFF_HISTORY_FILE)
        save_data_to_file(df_metrics_new, METRICS_HISTORY_FILE)

        # 3. Парсинг для НОВОГО ГРАФИКА задач (Секция 1)
        df_uncompleted_tasks_today, daily_overdue_data = parse_uncompleted_tasks_for_chart(report_text_input)

        # 4. Обновление накопительных данных о просроченных задачах за месяц
        # daily_overdue_data - словарь с просроченными задачами за сегодня {Менеджер: Кол-во}
        monthly_overdue_data = calculate_and_update_monthly_overdue(daily_overdue_data)

        # 5. Генерация НОВОГО двойного графика просроченных задач (График 1)
        chart_overdue_dual_file = generate_overdue_tasks_dual_chart(
            df_uncompleted_tasks_today,
            monthly_overdue_data,
            current_date
        )

        # 6. Загрузка всей истории для остальных графиков
        df_metrics_history = load_data_from_file(METRICS_HISTORY_FILE)

        # 7. Генерация старых графиков 2 и 3
        bot_charts_files = generate_data_dashboard_files(df_metrics_history, current_date)

        # 8. Генерация графиков по Google Sheets (4 и 5)
        gs_charts_files = download_and_process_google_sheet()

        # 9. --- ГРАФИК 6: Просроченные CRM задачи ---
        crm_chart_file = None
        if RETAILCRM_BASE_URL and RETAILCRM_API_KEY:
            start_date, end_date = get_month_range(current_date)

            # Получаем все задачи, созданные за месяц
            crm_tasks = fetch_retailcrm_tasks(RETAILCRM_BASE_URL, RETAILCRM_API_KEY, start_date, end_date)

            # Обрабатываем, фильтруем и считаем просроченные задачи
            overdue_data = process_tasks_for_chart_6(crm_tasks, RETAILCRM_BASE_URL, RETAILCRM_API_KEY)

            # Генерация графика
            crm_chart_file = generate_chart_6(overdue_data, current_date)
            print(f"✅ График 6 успешно сгенерирован: {crm_chart_file}")

        else:
            print("⚠️ График 6 пропущен: Отсутствуют RETAILCRM_BASE_URL или RETAILCRM_API_KEY в .env")

        # 10. Сбор всех файлов для слайдшоу
        all_bot_files = [chart_overdue_dual_file] + bot_charts_files + gs_charts_files
        if crm_chart_file:
            all_bot_files.append(crm_chart_file)

        # 11. Генерация файла-хоста слайдшоу (latest_dashboard.html)
        slideshow_host_file = generate_slideshow_host(all_bot_files, current_date)

        # 12. --- ВЫЗОВ ЗАГРУЗКИ НА ХОСТИНГ ---
        remote_path = os.getenv('SFTP_PATH', '/')

        # Добавляем OVERDUE_TASKS_MONTHLY_FILE в список для загрузки
        all_files_to_upload = all_bot_files + [slideshow_host_file, OVERDUE_TASKS_MONTHLY_FILE]
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
        generate_dashboard_from_text(report_text_input)
        print("\n✅ Генерация дашборда завершена.")
    except Exception as e:
        print(f"\n❌ Критическая ошибка при работе генератора: {e}")
