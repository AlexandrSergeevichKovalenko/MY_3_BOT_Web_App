import asyncio
import pandas as pd
import matplotlib
matplotlib.use('Agg') # <-- Встановити неінтерактивний бекенд
import matplotlib.pyplot as plt # <-- Імпортувати pyplot ПІСЛЯ встановлення бекенду
import numpy as np
from backend.analytics import fetch_user_timeseries


async def prepare_aggregate_data_by_period_and_draw_analytic_for_user(user_id, start_date, end_date):
    """Loads pre-aggregated daily analytics based on actual translations."""
    loop = asyncio.get_running_loop()
    points = await loop.run_in_executor(
        None,
        fetch_user_timeseries,
        user_id,
        start_date,
        end_date,
        "day",
    )

    if not points:
        return pd.DataFrame()

    df = pd.DataFrame(points)
    df.rename(
        columns={
            "period_start": "date",
            "avg_time_min": "avg_min_per_translation",
        },
        inplace=True,
    )
    df["date"] = pd.to_datetime(df["date"]).dt.date
    return df

async def aggregate_data_for_charts(df: pd.DataFrame, period: str = "week") -> pd.DataFrame:
    """
    Aggregates already-prepared daily analytics into requested period.
    """
    if df.empty:
        return df

    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    period_mappers = {
        "day": df["date"].dt.to_period("D"),
        "week": df["date"].dt.to_period("W"),
        "month": df["date"].dt.to_period("M"),
        "quarter": df["date"].dt.to_period("Q"),
        "half-year": df["date"].dt.to_period("Q"),
        "year": df["date"].dt.to_period("Y"),
    }
    if period not in period_mappers:
        raise ValueError("Used incorrected grouped period. Please use 'day', 'week', 'month', 'quarter', 'half-year' або 'year'.")

    if period == "half-year":
        half = df["date"].dt.month.apply(lambda m: 1 if m <= 6 else 2)
        year = df["date"].dt.year
        grouper = pd.PeriodIndex(year.astype(str) + "-H" + half.astype(str), freq="2Q")
    else:
        grouper = period_mappers[period]

    df["total_time_min"] = df.get("total_time_min", 0)
    df["avg_score"] = df.get("avg_score", 0)
    df["success_on_1st_attempt"] = df.get("success_on_1st_attempt", 0)
    df["success_on_2nd_attempt"] = df.get("success_on_2nd_attempt", 0)
    df["success_on_3plus_attempt"] = df.get("success_on_3plus_attempt", 0)

    grouped = df.groupby(grouper).apply(
        lambda group: pd.Series(
            {
                "total_translations": group["total_translations"].sum(),
                "successful_translations": group["successful_translations"].sum(),
                "unsuccessful_translations": group["unsuccessful_translations"].sum(),
                "success_on_1st_attempt": group["success_on_1st_attempt"].sum(),
                "success_on_2nd_attempt": group["success_on_2nd_attempt"].sum(),
                "success_on_3plus_attempt": group["success_on_3plus_attempt"].sum(),
                "total_time_min": group["total_time_min"].sum(),
                "avg_score": (
                    (group["avg_score"] * group["total_translations"]).sum()
                    / group["total_translations"].sum()
                    if group["total_translations"].sum() > 0
                    else 0
                ),
            }
        )
    )

    grouped["avg_min_per_translation"] = grouped.apply(
        lambda row: round(row["total_time_min"] / row["total_translations"], 2) if row["total_translations"] > 0 else 0,
        axis=1,
    )

    return grouped


def plot_user_analytics(ax, df, title, chart_type='time_and_success'):
    """
    Малює один аналітичний графік на вказаній осі (ax).

    Args:
        ax (matplotlib.axes.Axes): Вісь, на якій потрібно малювати.
        df (pd.DataFrame): Агреговані дані для побудови.
        title (str): Заголовок для графіка.
        chart_type (str): Тип графіка ('time_and_success' або 'attempts').
    """
    # Готуємо дані для осі X
    x_labels = df.index.astype(str)
    x = np.arange(len(x_labels)) 

    if chart_type == 'time_and_success':
        # --- Графік 1: Доля успішних/неуспішних і час ---

        # Малюємо стовпчасту діаграму
        ax.bar(x, df['successful_translations'], width=0.6, label="Successful(>=80)", color="g")
        ax.bar(x, df['unsuccessful_translations'], width=0.6, bottom=df['successful_translations'], label="Unsuccessful(<80)", color="r")
        ax.set_ylabel("Number of translations")
        ax.legend(loc="upper left")

        # Створюємо другу вісь Y для графіка часу
        ax2 = ax.twinx()
        ax2.plot(x, df['avg_min_per_translation'], color="b", marker='o', linestyle='--', label= "Average time (min) per each translation")
        ax2.set_ylabel("Minutes", color="b")
        ax2.tick_params(axis="y", labelcolor="b")
        ax2.legend(loc="upper right")
    
    elif chart_type == "attempts":
        # --- Графік 2: Аналіз за спробами ---
        ax.bar(x, df['success_on_1st_attempt'], width=0.6, label="Success from the 1 try", color='#2ca02c')
        ax.bar(x, df['success_on_2nd_attempt'], width=0.6, label="Success from the 2 try", 
                bottom=df['success_on_1st_attempt'],color='#ff7f0e')
        bottom_3 = df['success_on_1st_attempt'] +df['success_on_2nd_attempt']
        ax.bar(x, df['success_on_3plus_attempt'], width=0.6, bottom=bottom_3,
            label='Success from the 3 try', color='#1f77b4')
        bottom_4 = bottom_3 + df['success_on_3plus_attempt']
        ax.bar(x, df['unsuccessful_translations'], width=0.6, bottom=bottom_4,
            label='Неуспішні', color='#d62728') # червоний
        
        ax.set_ylabel("Number of translations")
        ax.legend(loc="best")

    ax.set_title(title, fontsize=14)
    ax.set_xticks(x)
    ax.set_xticklabels(x_labels, rotation=45, ha="right")
    ax.grid(True,axis="x", linestyle="--", alpha=0.7)
    

async def create_analytics_figure_async(daily_data, weekly_data, user_id):
    """
    Async shell for the creation of the bar-chart
    """
    loop = asyncio.get_running_loop()
    
    fig, axes = plt.subplots(2,1, figsize=(14,12))

    await loop.run_in_executor(
        None,
        plot_user_analytics,
        axes[0],
        daily_data.tail(7),
        "Daily Analytics: Time and Success",
        'time_and_success'
    )

    await loop.run_in_executor(
        None,
        plot_user_analytics,
        axes[1],
        weekly_data.tail(4),
        "Weekly Analytics: Tries",
        "attempts"
    )

            
    # Додаємо загальний заголовок
    fig.suptitle(f"The whole Analytics for the user {user_id}", fontsize=16)

    # Робимо вигляд компактнішим
    plt.tight_layout(rect=[0, 0, 1, 0.96]) # Залишаємо місце для suptitle

    #save in file to send it to telegram
    figure_path = f"analytics_{user_id}.png"
    fig.savefig(figure_path)
    plt.close(fig)

    return figure_path


