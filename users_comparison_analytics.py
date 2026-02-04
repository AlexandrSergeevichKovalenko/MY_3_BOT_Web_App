from load_data_from_db import get_db_connection
from user_analytics import aggregate_data_for_charts
import pandas as pd
import numpy as np
import asyncio
from user_analytics import prepare_aggregate_data_by_period_and_draw_analytic_for_user
import matplotlib.pyplot as plt


async def prepare_comparison_data(start_date, end_date):
    """
    Готовит данные для сравнительных графиков по всем пользователям.
    Возвращает две "широкие" таблицы (daily и weekly) с многоуровневыми столбцами.
    """
    daily_reports_list = []
    weekly_reports_list = []

    with get_db_connection() as connection:
        with connection.cursor() as curr:
            curr.execute("""
                SELECT DISTINCT user_id, username
                FROM bt_3_translations;
            """)

            all_users = curr.fetchall()
    
    for user_id, username in all_users:
        full_user_data = await prepare_aggregate_data_by_period_and_draw_analytic_for_user(user_id, start_date, end_date)
        if not full_user_data.empty:
            daily_data = await aggregate_data_for_charts(full_user_data, period="day")
            weekly_data = await aggregate_data_for_charts(full_user_data, period="week")

            daily_data["username"] = username
            daily_data.reset_index(inplace=True)

            weekly_data["username"] = username
            weekly_data.reset_index(inplace=True)

            daily_reports_list.append(daily_data)
            weekly_reports_list.append(weekly_data)
    
    if not daily_reports_list or not weekly_reports_list:
        print("Warning: No data collected for one or both periods.")
        return pd.DataFrame(), pd.DataFrame()

    # Когда мы объединяем несколько маленьких таблиц (от каждого пользователя), 
    # каждая из них имеет свой собственный индекс (например, 0, 1, 2...). Если их просто сложить, итоговый индекс будет состоять из повторяющихся чисел.
    # ignore_index=True говорит pandas: "Забудь про старые индексы и создай один новый, чистый и последовательный индекс для итоговой таблицы".
    merged_daily_df = pd.concat(daily_reports_list, ignore_index=True)
    merged_weekly_df = pd.concat(weekly_reports_list, ignore_index=True)

    # Определяем список всех метрик, которые нам нужны
    metrics_to_pivot = [
        'total_translations', 'successful_translations', 
        'unsuccessful_translations', 'avg_min_per_translation'
    ]

    # Создаём "широкую" таблицу для дневных данных
    pivoted_merged_daily_df = merged_daily_df.pivot_table(
        index="date", ## Что будет строками в новой таблице?
        columns="username", ## Что станет столбцами?
        values= metrics_to_pivot # Какие значения будут в ячейках?
    )

    # Создаём "широкую" таблицу для недельных данных
    pivoted_merged_weekly_df = merged_weekly_df.pivot_table(
        index="date", ## Что будет строками в новой таблице?
        columns="username", ## Что станет столбцами?
        values= metrics_to_pivot# Какие значения будут в ячейках?

    )

    return pivoted_merged_daily_df, pivoted_merged_weekly_df



def plot_comparison_chart(ax, pivoted_df, title):
    # список цветов для функции линейной отражающей среднее время. Чтобы каждый пользователь был разным цветом отображен.
    colors = ['b', 'g', 'r', 'c', 'm', 'y', 'k']
    # Создаём вторую ось ОДИН РАЗ до цикла
    ax2 = ax.twinx()
    # 1. Получаем список пользователей и периодов из pivoted_df
    usernames = pivoted_df.columns.get_level_values('username').unique()
    periods = pivoted_df.index.astype(str)

    # 2. Определяем ширину одного столбика и количество пользователей
    bar_width = 0.2
    num_users = len(usernames)

    # 3. Создаём числовые позиции для ГРУПП столбиков (для каждого периода)
    x_positions = np.arange(len(periods))

    for i, user in enumerate(usernames):
        current_color = colors[i % len(colors)]
        # 4a. Рассчитываем смещение для текущего пользователя
        offset = bar_width*(i - num_users/2)
        # 4b. Получаем данные (высоту столбиков) ТОЛЬКО для этого пользователя
        total_sentences = pivoted_df[('total_translations', user)]
        successful = pivoted_df[("successful_translations", user)]
        unsuccessful = pivoted_df[("unsuccessful_translations", user)]
        avg_time = pivoted_df[("avg_min_per_translation", user)]
        
        # 4c. Рисуем столбики для этого пользователя со смещением
        ax.bar(x_positions+offset, successful, width=bar_width, label=user)
        ax.bar(x_positions+offset, unsuccessful, width=bar_width, bottom=successful)
        # обязательно необходимо показать также график времени plot
        
        ax2.plot(x_positions, avg_time, color=current_color, marker='o', linestyle='--', label=user)

    # 5. !!! ПОСЛЕ!!! цикла настраиваем внешний вид графика
    ax.set_title(title)#, fontsize=14)
    ax.set_xticks(x_positions)
    ax.set_xticklabels(periods, rotation=45, ha="right")
    ax.set_ylabel("Number of translations")
 
    # Matplotlib сам соберёт легенды из label'ов, которые мы добавили
    ax.legend(title="Users", loc="upper left")
    
    ax2.set_ylabel("Avg Time (min)", color="gray")
    ax2.tick_params(axis="y", labelcolor="gray")
    ax2.legend(title="Avg time per sent", loc="upper right")


async def create_comparison_report_async(start_date, end_date, period="week"):
    pivoted_daily_df, pivoted_weekly_df = await prepare_comparison_data(start_date, end_date)
    use_weekly = period != "day"
    df_for_plot = pivoted_weekly_df if use_weekly else pivoted_daily_df
    title = "Weekly Users comparison" if use_weekly else "Daily Users comparison"

    fig, ax = plt.subplots(figsize=(15,8))

    loop = asyncio.get_running_loop()
    await loop.run_in_executor(
        None,
        plot_comparison_chart,
        ax,
        df_for_plot,
        title
    )

    file_with_analytics = f"comparison{period}.png"
    fig.savefig(file_with_analytics)
    plt.close(fig)

    return file_with_analytics













    
    
        


    
