import os
import logging
import psycopg2
import pandas as pd
import asyncio

# === Подключение к базе данных PostgreSQL ===
DATABASE_URL = os.getenv("DATABASE_URL_RAILWAY")

if DATABASE_URL:
    logging.info("✅ DATABASE_URL успешно загружен!")
else:   
    logging.error("❌ Ошибка: DATABASE_URL не задан. Проверь переменные окружения!")

def get_db_connection():
    return psycopg2.connect(DATABASE_URL, sslmode='require')

# NB: there used to be a connectivity smoke check right here — it opened a real connection
# at IMPORT time just to print the server version. bot_3 imports this module, so every bot
# start (and every test run, and every tooling import) paid for a database round-trip before
# a single line of work, and a slow database turned a plain import into a timeout. The
# functions below open their own connections when they are actually used.

def load_data_for_analytics(user_id: int, start_date, end_date, period: str = 'week') -> pd.DataFrame:
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT user_id, id_for_mistake_table, attempt FROM bt_3_attempts
                WHERE user_id = %s AND timestamp::date BETWEEN %s AND %s;
            """,(user_id, start_date, end_date))

            result_from_bt_3_attempts = cursor.fetchall()
            columns_bt_3_attempts = [desc[0] for desc in cursor.description]
            df_not_succeded_attempts = pd.DataFrame(result_from_bt_3_attempts, columns=columns_bt_3_attempts)

            cursor.execute("""
                SELECT session_id, username, start_time, end_time FROM bt_3_user_progress
                WHERE user_id = %s AND end_time::date BETWEEN %s AND %s;
            """, (user_id, start_date, end_date))
            
            result_from_user_progress_deepseek = cursor.fetchall()

            columns_user_progress_deepseek = [desc[0] for desc in cursor.description]
            df_progress = pd.DataFrame(result_from_user_progress_deepseek, columns=columns_user_progress_deepseek)

            cursor.execute("""
                SELECT session_id, username, sentence_id, score, timestamp FROM bt_3_translations
                WHERE user_id = %s AND timestamp::date BETWEEN %s AND %s;
            """, (user_id, start_date, end_date ))
            result_translations_deepseek = cursor.fetchall()          
            columns_translations_deepseek = [desc[0] for desc in cursor.description]
            df_translations = pd.DataFrame(result_translations_deepseek, columns=columns_translations_deepseek)

            cursor.execute("""
                SELECT sentence_id, score, attempt, date FROM bt_3_successful_translations
                WHERE user_id = %s AND date::date BETWEEN %s AND %s;
            """, (user_id, start_date, end_date))
            result_success_translations = cursor.fetchall()          
            columns_success = [desc[0] for desc in cursor.description]
            df_success = pd.DataFrame(result_success_translations, columns=columns_success)

            cursor.execute("""
                SELECT sentence_id, score FROM bt_3_detailed_mistakes
                WHERE user_id = %s AND added_data::date BETWEEN %s AND %s;
            """, (user_id, start_date, end_date ))
            
            result_mistakes = cursor.fetchall()          
            columns_mistakes = [desc[0] for desc in cursor.description]
            df_mistakes = pd.DataFrame(result_mistakes, columns=columns_mistakes)

            cursor.execute("""
                SELECT date, id, session_id, user_id, id_for_mistake_table FROM bt_3_daily_sentences
                WHERE user_id = %s AND date::date BETWEEN %s AND %s;
            """, (user_id, start_date, end_date))

            result_sentences = cursor.fetchall()
            column_sentences = [desc[0] for desc in cursor.description]
            df_sentences = pd.DataFrame(result_sentences, columns=column_sentences)

            # Чтобы не Ловить ошибки в groupby, to_period() и других операциях:
            df_progress['start_time'] = pd.to_datetime(df_progress['start_time'])
            df_progress['end_time'] = pd.to_datetime(df_progress['end_time'])

            df_translations['timestamp'] = pd.to_datetime(df_translations['timestamp'])
            df_success['date'] = pd.to_datetime(df_success['date'])
            #df_mistakes['added_data'] = pd.to_datetime(df_mistakes['added_data'])
            #df_mistakes['first_seen'] = pd.to_datetime(df_mistakes['first_seen'])
            #df_mistakes['last_seen'] = pd.to_datetime(df_mistakes['last_seen'])
            df_sentences['date'] = pd.to_datetime(df_sentences['date'])


    return {
        "progress": df_progress,
        "translations": df_translations,
        "success": df_success,
        "mistakes": df_mistakes,
        "sentences": df_sentences,
        "not_succesed_attempts": df_not_succeded_attempts
    }



